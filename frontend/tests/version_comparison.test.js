import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const pageSource = readFileSync(new URL('../static/js/version-comparison.js', import.meta.url), 'utf8');
const viewerSource = readFileSync(new URL('../static/js/version-comparison-viewer.js', import.meta.url), 'utf8');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

function vueStub() {
    // Минимальные заменители реактивности: достаточно, чтобы выполнить setup.
    const mounted = [];
    return {
        mounted,
        Vue: {
            ref: value => ({value}),
            reactive: obj => obj,
            computed: fn => ({get value() { return fn(); }}),
            watch: () => {},
            nextTick: () => Promise.resolve(),
            onMounted: fn => mounted.push(fn),
            onBeforeUnmount: () => {},
        },
    };
}

function loadModules(extra = {}) {
    const {Vue, mounted} = vueStub();
    const sandbox = {Vue, fetch: async () => ({ok: true, json: async () => ({})}),
        localStorage: {getItem: () => null, setItem: () => {}}, ...extra};
    sandbox.globalThis = sandbox;
    vm.runInNewContext(viewerSource, sandbox);
    vm.runInNewContext(pageSource, sandbox);
    const components = {};
    const fakeApp = {component: (name, def) => { components[name] = def; }};
    sandbox.VersionComparisonViewer.register(fakeApp);
    sandbox.VersionComparison.register(fakeApp);
    return {sandbox, components, mounted};
}

const RESULT = {
    status: 'different', status_label: 'Есть отличия', identical_content: false, reasons: [],
    a: {page: 1, sheet_no: '1', title: 'План'}, b: {page: 1, sheet_no: '1', title: 'План'},
    canvas: {width_px: 4000, height_px: 2000, dpi: 200, origin_pt: [0, 0]},
    params: {dpi: 200, tol_pt: 0.5}, channels: {}, zones: [], overlays: [],
    annotations: {added: [], removed: [], changed: []}, transform: {identity: true, method: 'anchors'},
    regions: [
        {id: 'r1', class: 'text', bbox_px: [10, 10, 20, 20], text_before: ['1'], text_after: ['2'], moved_text: []},
        {id: 'r2', class: 'style', bbox_px: [100, 100, 140, 104], text_before: [], text_after: [], moved_text: []},
        {id: 'r3', class: 'removed', bbox_px: [300, 300, 340, 340], text_before: [], text_after: [], moved_text: []},
    ],
};

function mountViewer() {
    let resize = null;
    class ResizeObserver { constructor(cb) { resize = cb; } observe() {} disconnect() {} }
    const {sandbox, components, mounted} = loadModules({ResizeObserver});
    sandbox.VersionComparison.api = async () => ({result: JSON.parse(JSON.stringify(RESULT))});
    const state = components['vc-viewer'].setup({objectId: 'obj', run: {run_id: 'run1', a: {}, b: {}},
        items: [{spid: 's1', title: 'План'}], startSpid: 's1'});
    return {state, mounted, resize: () => resize()};
}

const flush = () => new Promise(r => setTimeout(r, 0));

describe('Сравнение версий — подключение к порталу', () => {
    it('пункт меню, раздел и скрипты подключены перед app.js', () => {
        expect(html).toContain("navigate('/version-comparison')");
        expect(html).toContain('<version-comparison-page></version-comparison-page>');
        const viewerTag = html.indexOf('/static/js/version-comparison-viewer.js');
        const pageTag = html.indexOf('/static/js/version-comparison.js');
        const appTag = html.indexOf('/static/js/app.js');
        expect(viewerTag).toBeGreaterThan(0);
        expect(viewerTag).toBeLessThan(pageTag);
        expect(pageTag).toBeLessThan(appTag);
        expect(app).toContain("hash === '/version-comparison'");
        expect(app).toContain('window.VersionComparison.register(app)');
    });

    it('модули регистрируют страницу и просмотрщик, API — свой префикс', () => {
        const {sandbox, components} = loadModules();
        expect(sandbox.VersionComparison.API).toBe('/api/version-comparison');
        expect(Object.keys(components).sort()).toEqual(['vc-viewer', 'version-comparison-page']);
        // Модули не обращаются к сети, кроме своего API.
        for (const src of [pageSource, viewerSource]) {
            expect(src).not.toMatch(/openrouter|anthropic|openai|\/api\/stage-comparison/i);
        }
    });

    it('статус «Нужна проверка» не выдаётся за «Совпадает»', () => {
        const {sandbox} = loadModules();
        expect(sandbox.VersionComparison.statusInfo('needs_review').label).toBe('Нужна проверка');
        expect(sandbox.VersionComparison.statusInfo('same').label).toBe('Совпадает');
        expect(sandbox.VersionComparison.statusInfo('unmatched').label).toBe('Не сопоставлен');
        expect(sandbox.VersionComparison.statusInfo('removed').label).toBe('Лист удалён');
    });
});

describe('Сравнение версий — строка прогона', () => {
    it('оформление линий показывается отдельно от основных отличий', () => {
        const {components} = loadModules();
        const page = components['version-comparison-page'].setup();
        expect(page.itemLabel({kind: 'compare', status: 'done', result_status: 'different', regions: 137,
            style_regions: 120})).toBe('Есть отличия · 17 (+120 оформл.)');
        expect(page.itemLabel({kind: 'compare', status: 'done', result_status: 'different', regions: 4,
            style_regions: 4})).toBe('Есть отличия (+4 оформл.)');
        expect(page.itemLabel({kind: 'compare', status: 'done', result_status: 'different', regions: 3}))
            .toBe('Есть отличия · 3');
        expect(page.itemLabel({kind: 'compare', status: 'running'})).toBe('считается…');
    });
});

describe('Сравнение версий — просмотрщик', () => {
    it('шаблон: режимы, слой оформления, рамки', () => {
        const {components} = loadModules();
        const t = components['vc-viewer'].template;
        for (const s of ['рамки отличий', 'Вписать', 'оформление линий'])
            expect(t).toContain(s);
        const {state} = mountViewer();
        expect(state.modes.map(m => m.label)).toEqual(['Наложение', 'A', 'B', 'Рядом']);
        expect(state.layers.map(l => l.key)).toContain('style');
    });

    it('оформление скрыто в списке по умолчанию, но посчитано; навигация по видимым', async () => {
        const {state, mounted} = mountViewer();
        mounted.forEach(fn => fn());
        await flush();
        expect(state.mainCount.value).toBe(2);
        expect(state.styleCount.value).toBe(1);
        expect(state.listed.value.map(r => r.id)).toEqual(['r1', 'r3']);
        state.step(1);
        expect(state.selected.value).toBe('r1');
        state.step(1);
        expect(state.selected.value).toBe('r3');
        state.step(1);
        expect(state.selected.value).toBe('r1');
        state.showStyle.value = true;
        expect(state.listed.value.map(r => r.id)).toEqual(['r1', 'r2', 'r3']);
    });

    it('лист вписывается по реальному размеру панели, пока вид не трогали', async () => {
        const {state, mounted, resize} = mountViewer();
        mounted.forEach(fn => fn());
        await flush();
        state.paneEls.value = [{clientWidth: 1000, clientHeight: 500}];
        resize();
        expect(state.view.zoom).toBeCloseTo(0.245, 3);
        state.onWheel({ctrlKey: false, metaKey: false, shiftKey: false, deltaX: 0, deltaY: 50,
            currentTarget: {getBoundingClientRect: () => ({left: 0, top: 0})}});
        state.paneEls.value = [{clientWidth: 2000, clientHeight: 1000}];
        resize();
        expect(state.view.zoom).toBeCloseTo(0.245, 3);   // пользователь двигал вид — не перевписываем
        state.fitView();
        expect(state.view.zoom).toBeCloseTo(0.49, 3);
    });
});

describe('Сравнение версий — демонстрация 2', () => {
    it('текст «было → стало»: изменённые числа подсвечиваются, остальные — нет', async () => {
        const {state, mounted} = mountViewer();
        mounted.forEach(fn => fn());
        await flush();
        const tc = {numbers: [{before: '323.55', after: '324.26'}]};
        const segs = state.segments('Рр=323.55кВт Iр=12', tc, 'before');
        expect(segs.map(s => s.t).join('')).toBe('Рр=323.55кВт Iр=12');
        expect(segs.filter(s => s.hl).map(s => s.t)).toEqual(['323.55']);
        expect(state.normLabel('symbol')).toBe('замена символа лат./кир.');
        expect(state.fmtShift({shift_pt: [0, 12]})).toBe('0.0; +12.0 pt');
    });

    it('сводка переходов: кнопка в заголовке, «Открыть» выбирает пару и прогон перехода', async () => {
        const calls = [];
        const {components} = loadModules({fetch: async url => { calls.push(url); return {ok: true, json: async () => ({state: 'done', items: [], a: {}, b: {}})}; }});
        const t = components['version-comparison-page'].template;
        expect(t).toContain('Сравнить все соседние');
        expect(t).toContain('Сводка переходов');
        const page = components['version-comparison-page'].setup();
        const p = {project_id: 'p1', versions: ['v1', 'v2', 'v3']};
        page.openTransition(p, {a: 'v2', b: 'v3', run_id: 'run42'});
        expect(page.pairs.p1).toEqual({a: 'v2', b: 'v3'});
        await flush();
        expect(calls.some(u => u.endsWith('/runs/run42'))).toBe(true);
        expect(page.pairStateLabel({state: 'interrupted'})).toBe('прерван');
    });
});

describe('Сравнение версий — этап 2 в интерфейсе', () => {
    it('кнопки контроля состава и отчётов есть, кнопок СО и разрешения нет (сняты 15.10)', () => {
        const {components} = loadModules();
        const t = components['version-comparison-page'].template;
        for (const s of ['Контроль состава', 'Отчёты', 'HTML (ZIP)', 'Excel сводки'])
            expect(t).toContain(s);
        for (const s of ['СО по позициям', 'Разрешение:', 'permitView', 'specView'])
            expect(t).not.toContain(s);
        expect(components['vc-viewer'].template).toContain('Спецификация по позициям');
    });

    it('галочка «только изменения» прячет совпадающие строки выбранной пары', () => {
        const {components} = loadModules();
        const page = components['version-comparison-page'].setup();
        const p = {project_id: 'P'};
        const m = {rows: ['same', 'zones', 'diff', 'review', 'added', 'none', 'running'].map(id => ({row_id: id}))};
        page.pairs.P = {a: 'v1', b: 'v2'};
        page.activeRun.P = {scope: 'all', a: {version_id: 'v1'}, b: {version_id: 'v2'}, items: [
            {row_id: 'same', status: 'done', result_status: 'same'},
            {row_id: 'zones', status: 'done', result_status: 'same_zones_changed'},
            {row_id: 'diff', status: 'done', result_status: 'different'},
            {row_id: 'review', status: 'done', result_status: 'needs_review'},
            {row_id: 'added', status: 'done', result_status: 'added'},
            {row_id: 'running', status: 'running', kind: 'compare'}]};
        expect(page.visibleRows(p, m).length).toBe(7);
        page.onlyChanged.P = true;
        expect(page.visibleRows(p, m).map(r => r.row_id)).toEqual(['diff', 'review', 'added', 'running']);
        page.pairs.P.b = 'v3';  // прогона для пары нет — фильтр не действует
        expect(page.pairHasRun(p)).toBe(false);
        expect(page.visibleRows(p, m).length).toBe(7);
    });

    it('часть листа: пометки в матрице и подпись в строке прогона', () => {
        const {components} = loadModules();
        const page = components['version-comparison-page'].setup();
        const m = {parts: [{a_version: 'v1', a_page: 6, b_version: 'v2', b_page: 7, kind: 'split', a_title: 'Схема ВРУ-1'},
            {a_version: 'v1', a_page: 6, b_version: 'v2', b_page: 8, kind: 'split', a_title: 'Схема ВРУ-1'}]};
        expect(page.partOf(m, 'v2', 7).a_page).toBe(6);
        expect(page.partOf(m, 'v2', 9)).toBe(null);
        expect(page.partsFrom(m, 'v1', 6).map(x => x.b_page)).toEqual([7, 8]);
        expect(page.partTitle(m.parts[0])).toContain('Часть листа «Схема ВРУ-1»');
        expect(page.itemLabel({kind: 'compare', status: 'done', result_status: 'same', part: 'split', part_coverage: 0.97}))
            .toBe('Совпадает (часть листа, покрытие 97%)');
        expect(page.itemLabel({kind: 'compare', status: 'done', result_status: 'same_zones_changed', part: 'floors'}))
            .toBe('Совпадает, изменены служебные зоны (этаж типового листа)');
    });
});

it('keeps the curtain handle visible after zoom and panning beyond every edge', () => {
    const {state} = mountViewer();
    state.result.value = {canvas: {width_px: 2000, height_px: 1000}};
    Object.assign(state.paneSize, {w: 800, h: 600});
    for (const zoom of [0.1, 1, 8]) {
        for (const ox of [-10000, 0, 10000]) {
            for (const oy of [-10000, 0, 10000]) {
                Object.assign(state.view, {zoom, ox, oy});
                expect(state.swipeX()).toBeGreaterThanOrEqual(16);
                expect(state.swipeX()).toBeLessThanOrEqual(784);
                expect(state.handleY()).toBe(570);
            }
        }
    }
    Object.assign(state.view, {zoom: 1, ox: 500, oy: 200});
    const ev = x => ({clientX: x, pointerId: 1, currentTarget: {
        parentElement: {getBoundingClientRect: () => ({left: 0})}, setPointerCapture() {},
    }});
    state.swipeDown(ev(16));
    state.swipeMove(ev(400));
    expect(state.swipeX()).toBe(400);
});

it('clicking colored text selects and scrolls to its numbered change without boxes or a viewport jump', async () => {
    const {state} = mountViewer();
    state.result.value = {canvas: {width_px: 2000, height_px: 1000, dpi: 72, origin_pt: [0, 0]},
        regions: [{id: 'r7', class: 'text', bbox_px: [10, 10, 15, 20], text_changes: ['t7']}],
        text: {changes: [{id: 't7', a_bbox: [10, 10, 20, 20], b_in_a_bbox: [100, 100, 130, 110]}]}};
    Object.assign(state.view, {zoom: 2, ox: 50, oy: 50});
    state.showBoxes.value = false;
    let scrolled = false;
    state.rootEl.value = {querySelectorAll: () => [{dataset: {changeId: 'r7'}, scrollIntoView: () => {scrolled = true;}}]};
    const target = {dataset: {mode: 'b'}, getBoundingClientRect: () => ({left: 0, top: 0}), setPointerCapture() {}};
    const ev = (x, y, type = 'pointerup') => ({clientX: x, clientY: y, type, pointerId: 1, currentTarget: target});
    const before = {...state.view};
    state.onDown(ev(120, 110)); state.onUp(ev(120, 110));
    await flush();
    expect(state.selected.value).toBe('r7');
    expect(scrolled).toBe(true);
    expect(state.view).toEqual(before);
    state.selected.value = null;
    state.onDown(ev(120, 110)); state.onMove(ev(160, 110)); state.onUp(ev(160, 110));
    expect(state.selected.value).toBe(null);
    state.onDown(ev(120, 110)); state.onUp(ev(120, 110, 'pointercancel'));
    expect(state.selected.value).toBe(null);
});
