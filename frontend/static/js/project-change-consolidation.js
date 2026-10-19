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

    // Evidence grouped into sheets: one entry per (side, document, page), in document
    // order then page order, each carrying every evidence that points at that sheet.
    function evidenceSheets(card, side) {
        const sheets = new Map();
        for (const e of arr(card?.evidence)) {
            if (e.side !== side) continue;
            const key = [str(e.document?.id) || str(e.document?.label), e.page || 0].join('|');
            // Assemblies: show the original document cipher and its own PDF page, as in the sources columns.
            const origin = e.origin;
            const label = str(origin ? origin.source?.document_code : e.document?.label) || str(e.document?.label) || '—';
            const sourcePage = Number(origin ? origin.page?.source_page : e.page) || Number(e.page) || 0;
            if (!sheets.has(key)) sheets.set(key, {key: side + '|' + key, side, label, page: sourcePage,
                assemblyPage: origin ? Number(e.page) || 0 : 0, types: [], items: []});
            const sheet = sheets.get(key);
            sheet.items.push(e);
            if (e.source_type && !sheet.types.includes(e.source_type)) sheet.types.push(e.source_type);
        }
        return [...sheets.values()].sort((a, b) => a.label.localeCompare(b.label, 'ru') || a.page - b.page);
    }

    // Box checks shared with ProjectChangeView's page viewer: normalised 0..1 boxes on a page.
    function validPageView(data, evidenceId) {
        return Boolean(data) && data.evidence_id === evidenceId && /^\/(?!\/)/.test(data.image_url || '')
            && !/[\s\\]/.test(data.image_url) && Number.isFinite(data.page_width) && data.page_width > 0
            && Number.isFinite(data.page_height) && data.page_height > 0 && Array.isArray(data.highlights)
            && data.highlights.every(b => [b.x, b.y, b.width, b.height].every(Number.isFinite) && b.x >= 0 && b.y >= 0
                && b.width > 0 && b.height > 0 && b.x + b.width <= 1.001 && b.y + b.height <= 1.001);
    }

    // Width of label text in sheet units, in the font of .pcc-mark-label (a rough estimate without a canvas).
    function labelMeasure(size) {
        const ctx = root.document?.createElement?.('canvas')?.getContext?.('2d');
        if (!ctx) return text => text.length * size * .68;
        ctx.font = '700 ' + size + 'px system-ui, sans-serif';
        return text => ctx.measureText(text).width;
    }

    // One evidence sheet: shown open as a preview (the sheet with the referenced place marked,
    // scrolled to it); a click opens it in a window over the page, zoomable.
    const SheetView = {
        props: {sheet: {type: Object, required: true}, change: {type: String, default: ''},
            index: {type: Number, default: 0}, total: {type: Number, default: 1}},
        emits: ['step'],
        setup(props, {emit}) {
            const {ref, computed, nextTick, onMounted} = root.Vue;
            const loading = ref(false), error = ref(''), imageFailed = ref(false);
            const page = ref(null);       // {image_url, page_width, page_height}
            const boxes = ref([]);        // [{x, y, width, height, graphic, label}]
            const zoom = ref(1), dialog = ref(null), viewport = ref(null), plane = ref(null);
            const preview = ref(null), previewPlane = ref(null);
            let request = 0;
            const graphic = e => e.source_type === 'GRAPHIC';
            async function load() {
                const id = ++request;
                loading.value = true; error.value = ''; imageFailed.value = false; page.value = null; boxes.value = [];
                const found = [];
                let base = null;
                for (const e of props.sheet.items) {
                    if (!e.page_view_url) continue;
                    try {
                        const response = await root.fetch(e.page_view_url);
                        if (!response.ok) throw new Error('page unavailable');
                        const data = await response.json();
                        if (!validPageView(data, e.id)) throw new Error('invalid page location');
                        base = base || data;
                        for (const b of data.highlights) {
                            // A place already marked by another evidence is only outlined: stacked fills hide the text.
                            const overlap = found.some(f => Math.min(f.x + f.width, b.x + b.width) - Math.max(f.x, b.x) > 0
                                && Math.min(f.y + f.height, b.y + b.height) - Math.max(f.y, b.y) > 0);
                            const same = found.some(f => [f.x - b.x, f.y - b.y, f.width - b.width, f.height - b.height]
                                .every(v => Math.abs(v) < .002));
                            if (!same) found.push({...b, graphic: graphic(e) || data.kind === 'SOURCE_BLOCK', outline: overlap,
                                label: 'Изменение: ' + props.change});
                        }
                    } catch (_e) { /* one failed evidence must not hide the others */ }
                }
                if (id !== request) return;
                page.value = base;
                boxes.value = found;
                if (!base) error.value = props.sheet.items.some(e => e.page_view_url)
                    ? 'Не удалось получить выделение — показан сохранённый фрагмент.'
                    : 'Для этого листа нет точного выделения — показан сохранённый фрагмент.';
                loading.value = false;
            }
            if (root.Vue.getCurrentInstance?.()) onMounted(load); else load();
            const fallbackImage = computed(() => props.sheet.items.find(e => e.image_url)?.image_url || '');
            const imageUrl = computed(() => (!imageFailed.value && page.value?.image_url) || fallbackImage.value);
            const overlay = computed(() => Boolean(page.value) && !imageFailed.value);
            const planeStyle = computed(() => ({width: (100 * zoom.value) + '%'}));
            // Bring the first marked place into view of a viewport (the quote is often below the fold).
            function focusMark(vp, pl) {
                const first = boxes.value[0];
                if (!vp || !pl || !first) return;
                vp.scrollTop = Math.max(0, (first.y + first.height / 2) * pl.clientHeight - vp.clientHeight / 2);
                vp.scrollLeft = Math.max(0, (first.x + first.width / 2) * pl.clientWidth - vp.clientWidth / 2);
            }
            const focusPreview = () => focusMark(preview.value, previewPlane.value);
            async function openWindow() {
                zoom.value = 1;
                if (!dialog.value?.open) dialog.value?.showModal?.();
                await nextTick();
                focusMark(viewport.value, plane.value);
            }
            async function setZoom(factor, event) {
                const vp = viewport.value, pl = plane.value;
                const before = pl?.getBoundingClientRect(), vr = vp?.getBoundingClientRect();
                const x = event?.clientX ?? (vr ? vr.left + vp.clientWidth / 2 : 0);
                const y = event?.clientY ?? (vr ? vr.top + vp.clientHeight / 2 : 0);
                const rx = before ? (x - before.left) / before.width : .5, ry = before ? (y - before.top) / before.height : .5;
                zoom.value = Math.max(1, Math.min(8, zoom.value * factor));
                await nextTick();
                const now = pl?.getBoundingClientRect();
                if (vp && now) { vp.scrollLeft += now.left + rx * now.width - x; vp.scrollTop += now.top + ry * now.height - y; }
            }
            function wheel(event) {
                if (!event.ctrlKey) return;
                event.preventDefault();
                const delta = event.deltaY * (event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? 600 : 1);
                return setZoom(Math.exp(-Math.max(-300, Math.min(300, delta)) * .003), event);
            }
            // Previous / next sheet of the same side (Было or Стало): the parent opens the neighbour's window.
            function step(delta) {
                const target = props.index + delta;
                if (target < 0 || target >= props.total) return;
                dialog.value?.close?.();
                emit('step', target);
            }
            function keydown(event) {
                if (zoom.value > 1 || event.altKey || event.ctrlKey || event.metaKey) return;
                if (event.key === 'ArrowLeft') { event.preventDefault(); step(-1); }
                if (event.key === 'ArrowRight') { event.preventDefault(); step(1); }
            }
            async function fit() { zoom.value = 1; await nextTick(); focusMark(viewport.value, plane.value); }
            const box = b => ({x: b.x * page.value.page_width, y: b.y * page.value.page_height,
                width: b.width * page.value.page_width, height: b.height * page.value.page_height});
            const labelSize = computed(() => page.value ? Math.max(7, page.value.page_width / 70) : 9);
            // The label is shown in full: wrapped into lines no wider than 60 % of the sheet (measured with
            // the label's own font), kept on the sheet, above the marked place when there is room, else below.
            const label = b => {
                const r = box(b), size = labelSize.value, W = page.value.page_width, H = page.value.page_height;
                const measure = labelMeasure(size), maxWidth = Math.max(size * 8, W * .6);
                const lines = [];
                for (const word of b.label.split(/\s+/).filter(Boolean)) {
                    const last = lines.length - 1;
                    if (last >= 0 && measure(lines[last] + ' ' + word) <= maxWidth) { lines[last] += ' ' + word; continue; }
                    let rest = word;
                    while (measure(rest) > maxWidth && rest.length > 1) {
                        let cut = rest.length - 1;
                        while (cut > 1 && measure(rest.slice(0, cut)) > maxWidth) cut--;
                        lines.push(rest.slice(0, cut)); rest = rest.slice(cut);
                    }
                    lines.push(rest);
                }
                const step = size * 1.25, height = lines.length * step + size * .5;
                const width = Math.min(W, Math.max(...lines.map(measure)) + size * .8);
                const x = Math.max(0, Math.min(r.x, W - width));
                const y = r.y - height - 2 >= 0 ? r.y - height - 2 : Math.min(r.y + r.height + 2, Math.max(0, H - height));
                return {x, y, width, height, tx: x + size * .4, size,
                    lines: lines.map((text, i) => ({text, y: y + size * 1.1 + i * step}))};
            };
            return {loading, error, imageFailed, page, boxes, zoom, dialog, viewport, plane, preview, previewPlane,
                str, imageUrl, overlay, planeStyle, focusPreview, openWindow, setZoom, wheel, fit, box, label, load, step, keydown};
        },
        template: `
            <article class="pcc-sheet">
                <button type="button" class="pcc-sheet-head" @click="openWindow" title="Открыть лист в окне">
                    <span class="pcc-sheet-title">{{ sheet.label }} · стр. {{ sheet.page }}</span>
                    <small v-if="sheet.assemblyPage" class="pc-meta">(стр. {{ sheet.assemblyPage }} сборки)</small>
                    <span v-for="t in sheet.types" :key="t" :class="'pcc-src-type pcc-src-type--' + t.toLowerCase()">{{ t }}</span>
                    <span class="pcc-sheet-open">Открыть ⤢</span>
                </button>
                <div ref="preview" class="pcc-sheet-preview" @click="openWindow" role="button" tabindex="0"
                    @keydown.enter="openWindow" :aria-label="'Открыть лист ' + sheet.label + ', стр. ' + sheet.page">
                    <p v-if="loading" class="pc-meta pcc-sheet-loading" role="status">Поиск выделения на листе…</p>
                    <div ref="previewPlane" class="pcc-sheet-plane">
                        <img v-if="imageUrl" :src="imageUrl" :alt="sheet.label + ', стр. ' + sheet.page" draggable="false"
                            loading="lazy" @load="focusPreview" @error="imageFailed = true">
                        <svg v-if="overlay" class="pcc-sheet-overlay" :viewBox="'0 0 ' + page.page_width + ' ' + page.page_height"
                            preserveAspectRatio="none" aria-hidden="true">
                            <template v-for="(b, i) in boxes" :key="i">
                                <rect v-bind="box(b)" :class="['pcc-mark', b.graphic ? 'pcc-mark--graphic' : 'pcc-mark--text', b.outline && 'pcc-mark--outline']" />
                                <template v-if="b.graphic">
                                    <rect class="pcc-mark-label-bg" :x="label(b).x" :y="label(b).y" :width="label(b).width" :height="label(b).height" rx="2" />
                                    <text v-for="(l, j) in label(b).lines" :key="j" class="pcc-mark-label" :x="label(b).tx" :y="l.y" :font-size="label(b).size">{{ l.text }}</text>
                                </template>
                            </template>
                        </svg>
                    </div>
                </div>
                <p v-if="error" class="pc-meta pcc-sheet-note" role="status">{{ error }}</p>
                <p v-if="sheet.items[0]?.quote" class="pcc-sheet-quote">{{ sheet.items[0].quote }}</p>
                <dialog ref="dialog" class="pcc-sheet-dialog" @click="$event.target === dialog && dialog.close()" @keydown="keydown">
                    <header class="pcc-sheet-dialog-head">
                        <span v-if="total > 1" class="pc-meta">Лист {{ index + 1 }} из {{ total }}</span>
                        <strong>{{ sheet.side === 'OLD' ? 'Было (ПД)' : 'Стало (РД)' }} · {{ sheet.label }} · стр. {{ sheet.page }}</strong>
                        <span v-for="t in sheet.types" :key="t" :class="'pcc-src-type pcc-src-type--' + t.toLowerCase()">{{ t }}</span>
                        <span class="pcc-sheet-tools">
                            <button type="button" class="btn btn-sm" @click="setZoom(1 / 1.25)" aria-label="Уменьшить лист">−</button>
                            <span>{{ Math.round(zoom * 100) }}%</span>
                            <button type="button" class="btn btn-sm" @click="setZoom(1.25)" aria-label="Увеличить лист">+</button>
                            <button type="button" class="btn btn-sm" @click="fit">Вписать</button>
                            <span class="pc-meta">Ctrl + колесо — масштаб</span>
                        </span>
                        <button type="button" class="btn btn-sm" @click="dialog.close()" autofocus>Закрыть</button>
                    </header>
                    <div class="pcc-sheet-stage">
                    <button v-if="total > 1" type="button" class="pcc-sheet-nav pcc-sheet-nav--prev" :disabled="index === 0"
                        @click="step(-1)" :aria-label="'Предыдущий лист ' + (sheet.side === 'OLD' ? '«Было»' : '«Стало»')" title="Предыдущий лист (←)">‹</button>
                    <div ref="viewport" class="pcc-sheet-viewport" @wheel="wheel">
                        <div ref="plane" class="pcc-sheet-plane" :style="planeStyle">
                            <img v-if="imageUrl" :src="imageUrl" :alt="sheet.label + ', стр. ' + sheet.page" draggable="false">
                            <svg v-if="overlay" class="pcc-sheet-overlay" :viewBox="'0 0 ' + page.page_width + ' ' + page.page_height"
                                preserveAspectRatio="none" aria-hidden="true">
                                <template v-for="(b, i) in boxes" :key="i">
                                    <rect v-bind="box(b)" :class="['pcc-mark', b.graphic ? 'pcc-mark--graphic' : 'pcc-mark--text', b.outline && 'pcc-mark--outline']" />
                                    <template v-if="b.graphic">
                                        <rect class="pcc-mark-label-bg" :x="label(b).x" :y="label(b).y" :width="label(b).width" :height="label(b).height" rx="2" />
                                        <text v-for="(l, j) in label(b).lines" :key="j" class="pcc-mark-label" :x="label(b).tx" :y="l.y" :font-size="label(b).size">{{ l.text }}</text>
                                    </template>
                                </template>
                            </svg>
                        </div>
                    </div>
                    <button v-if="total > 1" type="button" class="pcc-sheet-nav pcc-sheet-nav--next" :disabled="index >= total - 1"
                        @click="step(1)" :aria-label="'Следующий лист ' + (sheet.side === 'OLD' ? '«Было»' : '«Стало»')" title="Следующий лист (→)">›</button>
                    </div>
                    <footer class="pcc-sheet-dialog-foot">
                        <p v-if="error" class="pc-meta" role="status">{{ error }}</p>
                        <ul class="pcc-sheet-quotes">
                            <li v-for="e in sheet.items" :key="e.id"><span :class="'pcc-src-type pcc-src-type--' + str(e.source_type).toLowerCase()">{{ e.source_type }}</span>
                                <strong class="pcc-sheet-quote-what">{{ e.short_explanation_ru }}</strong>
                                <blockquote v-if="e.quote">{{ e.quote }}</blockquote></li>
                        </ul>
                    </footer>
                </dialog>
            </article>`,
    };

    function register(app) {
        app.component('pcc-evidence-sheet', SheetView);
        app.component('project-change-consolidation', {
            props: {sessionId: {type: String, default: ''}, pairId: {type: String, default: ''}},
            emits: ['panel', 'open-evidence'],
            setup(props, {emit}) {
                const {ref, computed, watch} = root.Vue;
                // Sheets are mounted (and their pages fetched) only while a card's evidence block is open.
                const evidenceOpen = root.Vue.reactive({});
                // Sheet windows of one card side, so ‹ › in a window can open its neighbour.
                const sheetRefs = new Map();
                function sheetRef(cardId, side, index, el) {
                    const key = cardId + '|' + side;
                    if (!sheetRefs.has(key)) sheetRefs.set(key, []);
                    sheetRefs.get(key)[index] = el;
                }
                function stepSheet(cardId, side, index) {
                    sheetRefs.get(cardId + '|' + side)?.[index]?.openWindow?.();
                }
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
                    if (!canOperate) return;
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
                // Права вошедшего (portal_auth.js): viewer не запускает разбор моделью и не
                // загружает свой — только скачивает готовое (при праве скачивания).
                const canOperate = !(root.portalCan && !root.portalCan.operate());
                const canDownload = !(root.portalCan && !root.portalCan.download('stage_comparison'));
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
                    if (!canOperate) return false;
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
                    if (ready.value) { if (canDownload) download(); return; }
                    if (!canOperate) {
                        uploadMessage.value = 'Результат ещё не подготовлен администратором';
                        uploadErrors.value = [];
                        return;
                    }
                    await startRun(false);
                }
                async function recompute() {
                    if (!canOperate) return;
                    const manual = analysis.value?.source === 'manual';
                    const question = manual ? 'Загружен ручной разбор инженера. Заменить его разбором модели? Прежний уйдёт в историю.'
                        : 'Пересчитать весь разбор моделью заново (около 30–40 минут)?';
                    if (root.confirm && !root.confirm(question)) return;
                    await startRun(true);
                }
                async function cancelRun() {
                    if (!canOperate) return;
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
                    evidenceSheets, evidenceOpen, sheetRef, stepSheet,
                    reportBase, analysis, uploading, uploadMessage, uploadErrors, uploadAnalysis,
                    modelRun, running, starting, ready, download, makeReport, recompute, cancelRun, elapsed, pollRun,
                    canOperate, canDownload,
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
                        <button v-if="canDownload" type="button" class="btn btn-sm btn-primary" @click="download('zip')"
                            title="Архив: Excel с готовыми результатами + папки «Было» и «Стало» с PDF и MD каждого документа, без запуска модели">Классический отчёт</button>
                        <button v-if="canOperate || canDownload" type="button" class="btn btn-sm btn-secondary" :disabled="running || starting" @click="makeReport"
                            :title="ready ? 'Скачать заполненный отчёт' : 'Разбор по корзинам моделью Claude Opus 5.5, затем отчёт'">
                            {{ running || starting ? 'ИИ-разбор выполняется…' : 'Отчёт с ИИ-разбором' }}</button>
                        <button v-if="canOperate && running" type="button" class="btn btn-sm btn-secondary" @click="cancelRun">Остановить</button>
                        <button v-if="canOperate && analysis?.available && !running" type="button" class="btn btn-sm btn-secondary"
                            @click="recompute">Пересчитать моделью</button>
                        <label v-if="canOperate && !running" class="btn btn-sm btn-secondary" :class="{'is-disabled': uploading}"
                            title="xlsx с заполненным листом «Карточки ИТ» (ручной разбор главнее модели)">{{ uploading ? 'Загрузка…' : 'Загрузить разбор' }}
                            <input type="file" accept=".xlsx" hidden :disabled="uploading" @change="uploadAnalysis"></label>
                        <span v-if="running" class="pc-meta" role="status">Модель разбирает карточки: пачка
                            {{ (modelRun.batches_done || 0) + 1 }} из {{ modelRun.batches_total }}<template
                            v-if="modelRun.current"> ({{ modelRun.current.ids[0] }}…{{ modelRun.current.ids[modelRun.current.ids.length - 1] }})</template>
                            · прошло {{ elapsed }} мин · отчёт скачается сам</span>
                        <span v-else-if="analysis?.available" class="pc-meta">{{ analysis.source === 'model' ? 'Разбор моделью' : 'Разбор инженера' }}:
                            {{ analysis.filled }} из {{ analysis.rows }} карточек · {{ analysis.uploaded_at.slice(0, 10) }}</span>
                        <span v-else-if="canOperate" class="pc-meta">Классический отчёт доступен сразу. «Отчёт с ИИ-разбором» запустит модель (≈30–40 мин)
                            · <a :href="reportBase + '/report.xlsx'" download>пустой шаблон</a></span>
                        <span v-else class="pc-meta">ИИ-разбор готовит администратор.</span>
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
                    <div class="pc-table-scroll pcc-table-scroll" aria-label="Таблица итоговых изменений">
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
                        <details class="pcc-evidence" @toggle="evidenceOpen[c.id] = $event.target.open">
                            <summary><span class="pcc-evidence-title">Листы доказательств</span>
                                <span class="pcc-evidence-count pcc-side--old">Было · {{ evidenceSheets(c, 'OLD').length }}</span>
                                <span class="pcc-evidence-count pcc-side--new">Стало · {{ evidenceSheets(c, 'NEW').length }}</span>
                                <button type="button" class="pc-link pcc-evidence-compare" :disabled="!destination(c)"
                                    @click.prevent.stop="open(c)">Открыть пару в сравнении ↗</button></summary>
                            <div v-if="evidenceOpen[c.id]" class="pcc-evidence-cols">
                                <section v-for="side in ['OLD', 'NEW']" :key="side" :class="'pcc-evidence-col pcc-evidence-col--' + side.toLowerCase()"
                                    :aria-label="side === 'OLD' ? 'Было — листы' : 'Стало — листы'">
                                    <h4>{{ side === 'OLD' ? 'Было (ПД)' : 'Стало (РД)' }}</h4>
                                    <pcc-evidence-sheet v-for="(sh, i) in evidenceSheets(c, side)" :key="sh.key" :sheet="sh" :change="title(c)"
                                        :index="i" :total="evidenceSheets(c, side).length" :ref="el => sheetRef(c.id, side, i, el)"
                                        @step="j => stepSheet(c.id, side, j)"></pcc-evidence-sheet>
                                    <p v-if="!evidenceSheets(c, side).length" class="pc-meta">Листов нет.</p>
                                </section>
                            </div>
                        </details>
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
    return {CHANNELS, SCOPES, BASES, PARAM_STATUS, DECISIONS, pick, key, summary, destination, title, parameterSources, parameterTableRows, evidenceSheets, validPageView, register};
}));
