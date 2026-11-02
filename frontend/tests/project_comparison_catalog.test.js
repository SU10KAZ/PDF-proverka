import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../static/js/project-comparison-catalog.js', import.meta.url), 'utf8');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

function entry(overrides = {}) {
    return {
        catalog_entry_id: 'pcc1_' + 'a'.repeat(32), result_source: 'LIVE_RUN', result_status: 'COMPLETED_FROZEN',
        object_id: 'obj', object_name: 'Объект', session_id: 's', pair_id: 'p', run_id: 'r', title: 'ШИФР-НОВЫЙ',
        documents: {old: {document_code: 'ШИФР-СТАРЫЙ'}, new: {document_code: 'ШИФР-НОВЫЙ'}},
        engine: {engine_version: '3.5.2', model: 'claude-opus-5', model_display: 'Claude Opus 5', reasoning: 'xhigh'},
        frozen_at: '2026-09-21T09:59:29+00:00', counts: {projectchanges: 7, unresolved_hints: 2},
        human_mapping: {available: true, regions: 5, url: '/human-mapping/?object=obj&comparison=p'},
        open: {available: true, object_id: 'obj', session_id: 's', pair_id: 'p', source_run_id: 'r'},
        is_primary: true, variants_in_pair: 1, ...overrides,
    };
}

function load(fetchImpl) {
    // Minimal reactive stand-ins: enough to run the component's setup logic.
    const ref = value => ({value});
    const computed = fn => ({get value() { return fn(); }});
    const mounted = [];
    const watched = [];
    const timers = [], unmounted = [];
    const opened = [];
    const sandbox = {setTimeout: fn => { timers.push(fn); return timers.length; }, clearTimeout: () => {}, Vue: {ref, computed, onMounted: fn => mounted.push(fn), onUnmounted: fn => unmounted.push(fn), watch: (get, fn) => watched.push({get, fn})}, fetch: fetchImpl,
        open: (...args) => opened.push(args), localStorage: {getItem: () => null, setItem: () => {}}};
    sandbox.globalThis = sandbox;
    vm.runInNewContext(source, sandbox);
    let definition;
    sandbox.ProjectComparisonCatalog.register({component: (name, def) => { definition = {name, ...def}; }});
    return {sandbox, definition, mounted, opened, watched, timers, unmounted};
}

describe('All project comparisons — catalog component', () => {
    it('requests only the selected object and ignores a late response after switching objects', async () => {
        const requests = [];
        const {definition, mounted, watched} = load(url => new Promise(resolve => requests.push({url, resolve})));
        const props = {objectId: 'first'};
        const state = definition.setup(props, {emit: () => {}});
        const first = mounted[0]();
        expect(requests[0].url).toBe('/api/project-comparison/catalog?object_id=first');
        props.objectId = 'second';
        watched[0].fn();
        expect(requests[1].url).toBe('/api/project-comparison/catalog?object_id=second');
        requests[0].resolve({ok: true, json: async () => ({schema: 'project-comparison-catalog/1', entries: [entry()]})});
        await first;
        expect(state.catalog.value).toBeNull();
        expect(state.loading.value).toBe(true);
        requests[1].resolve({ok: true, json: async () => ({schema: 'project-comparison-catalog/1', entries: []})});
        await new Promise(resolve => setTimeout(resolve, 0));
        expect(state.loading.value).toBe(false);
        expect(state.entries.value).toEqual([]);
    });
    it('uses fixed catalog columns so completed results do not resize rows', () => {
        expect(source).toContain('<col class="pc-catalog__col-project">');
        expect(source).not.toContain('<col class="pc-catalog__col-docs">');
        expect(source).toContain('<col class="pc-catalog__col-model">');
        expect(load(async () => ({})).definition.template).not.toContain("? '\n' +");
    });

    it('indicates only a live comparison, including a new run beside a saved result', () => {
        const {sandbox, definition} = load(async () => ({}));
        const row = {left: {pdf_path: '/old'}, right: {pdf_path: '/new'},
            comparisonRun: {state: {status: 'RUNNING'}}};
        const saved = entry({documents: {old: row.left, new: row.right}});
        const [result] = sandbox.ProjectComparisonCatalog.pairEntries([saved], [row], 'obj', '', 'latest');
        expect(result.pending_pair).toBeUndefined();
        expect(sandbox.ProjectComparisonCatalog.isRunning(result)).toBe(true);
        for (const status of ['PAUSED', 'FAILED', 'REVIEW', 'COMPLETED']) {
            row.comparisonRun.state.status = status;
            expect(sandbox.ProjectComparisonCatalog.isRunning(result)).toBe(false);
        }
        row.comparisonRun.state = {status: 'RUNNING', orphaned_run: true};
        expect(sandbox.ProjectComparisonCatalog.isRunning(result)).toBe(false);
        expect(sandbox.ProjectComparisonCatalog.isRunning(entry())).toBe(false);
        expect(definition.template).toContain('aria-label="Сравнение выполняется"');
    });

    it('registers a pair overview with saved-result and launch actions', () => {
        const {definition} = load(async () => ({}));
        expect(definition.name).toBe('project-comparison-catalog');
        expect(definition.emits).toEqual(['open', 'open-pair', 'start', 'select-pair', 'select-all']);
        for (const column of ['Проект / раздел', 'Метод и модель', 'Дата', 'Изменения',
            'Требуют проверки', 'Статус', 'Запустить сравнение'])
            expect(definition.template).toContain(column);
        expect(definition.template).not.toContain('OLD → NEW');
        expect(definition.template).not.toContain('pc-catalog__docs');
        expect(definition.template).toContain('<th>Метод и модель</th>');
        expect(definition.template).not.toContain('<th>V3</th>');
        expect(definition.template).not.toContain('Смысловые блоки в шаге 2');
        expect(definition.template).toContain("{{ entry.pending_pair ? '—' : model(entry) }}");
        expect(definition.template).not.toContain('{{ entry.object_name }}');
        expect(definition.template).toContain('entry.counts.projectchanges');
        expect(definition.template).not.toContain('Human Mapping');
        // The button delegates to the existing launch dialog, never calls inference itself.
        expect(source).not.toMatch(/production\/run|method:\s*'POST'/);
        expect(definition.template).not.toMatch(/Проверенные сравнения|Диагностика|только просмотр|<summary/);
    });

    it('loads entries from the catalog API and emits the entry to open', async () => {
        const calls = [];
        const payload = {schema: 'project-comparison-catalog/1', entries: [entry()]};
        const {definition, mounted, opened} = load(async url => {
            calls.push(url);
            return {ok: true, json: async () => payload};
        });
        const emitted = [];
        const state = definition.setup({activeEntryId: '', opening: false, error: '', sessionId: 's',
            pairRows: [{left: {document_code: 'ШИФР-СТАРЫЙ'}, right: {}, pair: {id: 'p'}}]},
            {emit: (...args) => emitted.push(args)});
        await mounted[0]();
        expect(calls).toEqual(['/api/project-comparison/catalog']);
        expect(state.entries.value.map(e => e.title)).toEqual(['ШИФР-СТАРЫЙ']);
        expect(state.model(state.entries.value[0])).toBe('Claude Opus 5 · xhigh');
        state.open(state.entries.value[0]);
        expect(emitted[0][0]).toBe('open');
        state.open(state.entries.value[0], {target: 'blocks'});
        expect(emitted[1][2]).toEqual({target: 'blocks'});
        expect(state.openHumanMapping).toBeUndefined();
        expect(opened).toHaveLength(0);
    });

    it('lists only the current object results', async () => {
        const payload = {schema: 'project-comparison-catalog/1', entries: [
            entry({title: 'СВОЙ'}),
            entry({catalog_entry_id: 'pcc1_' + 'b'.repeat(32), object_id: 'other', title: 'ЧУЖОЙ'})]};
        const {definition, mounted} = load(async () => ({ok: true, json: async () => payload}));
        const pairRows = [{left: {document_code: 'СВОЙ'}, right: {}, pair: {id: 'p'}}];
        const state = definition.setup({objectId: 'obj', sessionId: 's', pairRows}, {emit: () => {}});
        await mounted[0]();
        expect(state.entries.value.map(e => e.title)).toEqual(['СВОЙ']);
        const empty = definition.setup({objectId: 'none'}, {emit: () => {}});
        await mounted[2]();
        expect(empty.entries.value).toEqual([]);
        expect(definition.template).toContain('В этом объекте пока нет пар для сравнения.');
    });

    it('rejects a response with a foreign contract', async () => {
        const {definition, mounted} = load(async () => ({ok: true, json: async () => ({entries: []})}));
        const state = definition.setup({}, {emit: () => {}});
        await mounted[0]();
        expect(state.loadError.value).toContain('Неверный контракт каталога');
        expect(state.entries.value).toEqual([]);
    });

    it('shows the recorded model, not a prompt-derived label', () => {
        const {sandbox} = load(async () => ({}));
        const C = sandbox.ProjectComparisonCatalog;
        expect(C.model(entry({engine: {model: 'gpt-6-astra', model_display: 'GPT-6 Astra', reasoning: 'xhigh'}})))
            .toBe('GPT-6 Astra · xhigh');
        expect(C.date(null)).toBe('—');
    });
});

describe('Catalog wiring in the comparison page', () => {
    it('is an additional navigation layer on step 1', () => {
        expect(html).toContain('/static/js/project-comparison-catalog.js?v={{js_version}}');
        expect(html).toContain('<project-comparison-catalog v-if="scTab === \'upload\'"');
        expect(html).toContain('@open="pcOpenCatalogEntry"');
        expect(html).toContain(':object-id="currentObjectId || \'\'"');
        expect(html).not.toContain('id="pc-catalog-focus"');  // плашка «Результат каталога» убрана
        expect(app).toContain('window.ProjectComparisonCatalog.register(app);');
        expect(html).toContain(':pair-rows="scVisiblePairRows"');
        expect(html).toContain('@start="scStartCatalogPair"');
    });

    it('opens a result through the normal pair workflow with a per-tab object', () => {
        const start = app.indexOf('async function pcOpenCatalogEntry');
        const body = app.slice(start, app.indexOf('\n        }\n', start));
        expect(body).toContain('storeObjectId(target.object_id)');
        expect(body).not.toContain('/objects/switch');
        expect(body).toContain('await scOpenPair(pair)');
        // Waits for the watcher's own session load instead of racing it with a second load.
        expect(body).toContain('await pcEnsureResultSession(target.session_id, target.object_id)');
        expect(body).toContain('await nextTick();');
        expect(body).toContain("scTab.value = 'diffs'");
        expect(body).not.toMatch(/production\/run|scRunProduction|scStartProduction/);
        // The change list shows only the opened catalog run of that pair.
        expect(app).toContain('changes.filter(c => c.source_run_id === focus.source_run_id)');
    });
});

describe('all pair rows alongside saved results', () => {
    const row = (index, extra = {}) => ({index, left: {pdf_path: '/old' + index, document_code: 'OLD-' + index},
        right: {pdf_path: '/new' + index, document_code: 'NEW-' + index}, ...extra});
    it('lists every source pair, matches older results by exact sources and keeps zero counts', () => {
        const {sandbox} = load(async () => ({}));
        const rows = [row(0), row(1), row(2, {right: null}), row(3, {left: null})];
        const saved = entry({documents: {old: rows[0].left, new: rows[0].right}, counts: {projectchanges: 0}});
        const entries = sandbox.ProjectComparisonCatalog.pairEntries([saved], rows, 'obj', 'Объект', 'latest');
        expect(entries).toHaveLength(2);
        expect(entries[0]).toMatchObject({...saved, title: 'OLD-0'});
        expect(entries[0].selection_pair).toBe(rows[0]);
        expect(entries[0].counts.projectchanges).toBe(0);
        expect(entries[1].pending_pair).toBe(rows[1]);
        expect(entries[1].engine).toEqual({});
        expect(entries[1].counts.projectchanges).toBeUndefined();
        expect(entries.some(e => e.pending_pair === rows[2] || e.pending_pair === rows[3])).toBe(false);
    });
    it('shows source rows immediately while catalog metadata is pending', async () => {
        for (const pairsFirst of [true, false]) {
            let reply;
            const {definition, mounted} = load(() => new Promise(resolve => { reply = resolve; }));
            const pair = row(0), saved = entry({documents: {old: pair.left, new: pair.right}});
            const props = {objectId: 'obj', pairRows: [pair, row(1)], pairsLoading: true};
            const state = definition.setup(props, {emit: () => {}});
            const pending = mounted[0]();
            if (pairsFirst) props.pairsLoading = false;
            expect(state.entries.value).toHaveLength(2);
            reply({ok: true, json: async () => ({schema: 'project-comparison-catalog/1', entries: [saved]})});
            await pending;
            if (!pairsFirst) {
                expect(state.overviewLoading.value).toBe(false);
                expect(state.entries.value).toHaveLength(2);
                props.pairsLoading = false;
            }
            expect(state.overviewLoading.value).toBe(false);
            expect(state.entries.value).toHaveLength(2);
        }
    });
    it('shows one row per configured pair and excludes historical variants and obsolete source pairs', () => {
        const {sandbox} = load(async () => ({}));
        const matching = row(0, {pair: {id: 'p'}});
        const main = entry();
        const variant = entry({catalog_entry_id: 'variant', is_primary: false});
        const oldVersion = entry({catalog_entry_id: 'old-version', open: {...main.open, session_id: 'older'},
            documents: {old: matching.left, new: {pdf_path: '/other-version'}}});
        const foreign = entry({catalog_entry_id: 'foreign', object_id: 'other'});
        const entries = sandbox.ProjectComparisonCatalog.pairEntries([main, variant, oldVersion, foreign],
            [matching, row(1)], 'obj', 'Объект', 's');
        expect(entries).toHaveLength(2);
        expect(entries[0].catalog_entry_id).toBe(main.catalog_entry_id);
        expect(entries[1].pending_pair.index).toBe(1);
        expect(sandbox.ProjectComparisonCatalog.pairEntries([main], [], 'obj', '', 's')).toEqual([]);
    });
    it('mirrors three IOS1.1 groups despite three saved runs of the first pair', () => {
        const {sandbox, definition} = load(async () => ({}));
        const rows = [0, 1, 2].map(i => row(i, {left: {pdf_path: '/ios11', document_code: 'ИОС1.1'},
            pair: {id: 'p' + i}, group: {}, members: Array(5).fill({})}));
        rows[0].comparisonRun = {state: {run_id: 'current'}};
        const saved = (i, id, extra = {}) => entry({catalog_entry_id: id, run_id: id,
            documents: {old: rows[i].left, new: rows[i].right},
            open: {session_id: 's', pair_id: 'p' + i}, ...extra});
        const history = [saved(0, 'old-a'), saved(0, 'old-b', {is_primary: false}),
            saved(0, 'current', {is_current: true}), saved(1, 'second'),
            entry({catalog_entry_id: 'obsolete-source'})];
        for (const results of [history, [...history].reverse()]) {
            const entries = sandbox.ProjectComparisonCatalog.pairEntries(results, rows, 'obj', '', 's');
            expect(entries).toHaveLength(3);
            expect(entries.map(e => e.selection_pair)).toEqual(rows);
            expect(entries.map(e => e.catalog_entry_id).slice(0, 2)).toEqual(['current', 'second']);
            expect(entries[2].pending_pair).toBe(rows[2]);
            expect(entries.every(e => e.title === 'ИОС1.1 · сборка из 5')).toBe(true);
        }
        expect(definition.template).toContain('pairStatus(entry.selection_pair)');
        expect(definition.template).not.toContain('Заморожен');
    });
    it('keeps additional assemblies beside their source in the catalog and launch queue', () => {
        const {sandbox} = load(async () => ({}));
        const primary = row(0);
        const next = row(1);
        const extra = row(2, {left: primary.left, secondary: true});
        const context = {scPairRows: {value: [primary, next, extra]},
            computed: fn => ({get value() { return fn(); }}), pcReadOnlySources: {value: false},
            SC_BATCH: {key: r => r.left && r.right}};
        vm.createContext(context);
        for (const [start, end] of [
            ['const scVisiblePairRows = computed(', '// Веер соединяет'],
            ['const scBatchSelectableRows = computed(', 'const scBatchSelectedRows = computed('],
        ]) {
            const fragment = app.slice(app.indexOf(start), app.indexOf(end, app.indexOf(start)));
            vm.runInContext(fragment.replace('const ', 'var '), context);
        }
        expect(context.scVisiblePairRows.value).toEqual([primary, extra, next]);
        expect(context.scBatchSelectableRows.value).toEqual([primary, extra, next]);
        const saved = entry({documents: {old: next.left, new: next.right}});
        const entries = sandbox.ProjectComparisonCatalog.pairEntries([saved],
            context.scVisiblePairRows.value, 'obj', 'Объект', 'latest');
        expect(entries.map(e => e.selection_pair)).toEqual([primary, extra, next]);
    });
    it('labels groups and updates placeholder rows reactively after upload', async () => {
        const {definition, mounted} = load(async () => ({ok: true, json: async () => ({schema: 'project-comparison-catalog/1', entries: []})}));
        const props = {objectId: 'obj', pairRows: [row(0, {group: {}, members: [{}, {}]})]};
        const state = definition.setup(props, {emit: () => {}});
        await mounted[0]();
        expect(state.entries.value[0].title).toBe('OLD-0 · сборка из 2');
        props.pairRows = [...props.pairRows, row(1)];
        expect(state.entries.value).toHaveLength(2);
        props.pairsLoading = true;
        expect(state.entries.value).toHaveLength(2);
    });
    it('emits the exact source row only when launching is allowed; loading and errors prevent launch', async () => {
        const {definition, mounted} = load(async () => ({ok: true, json: async () => ({schema: 'project-comparison-catalog/1', entries: []})}));
        const emitted = [], pair = row(0);
        const props = {objectId: 'obj', pairRows: [pair], canStart: () => true};
        const state = definition.setup(props, {emit: (...args) => emitted.push(args)});
        const pending = mounted[0]();
        state.start(state.entries.value[0]);
        expect(emitted).toHaveLength(0);
        await pending;
        state.start(state.entries.value[0]);
        expect(emitted).toEqual([['start', pair]]);
        props.canStart = () => false;
        state.start(state.entries.value[0]);
        props.canStart = () => true;
        state.loadError.value = 'Unavailable';
        state.start(state.entries.value[0]);
        expect(emitted).toHaveLength(1);
    });
    it('restores the current source session when returning from a historical result', () => {
        const start = app.indexOf('        watch(scTab, tab => {\n            // Opening a frozen result');
        const end = app.indexOf('        watch(() => scSession.value?.id', start);
        let callback, refreshed = 0;
        const c = {watch: (_source, fn) => { callback = fn; }, scTab: {},
            pcCatalogSourceSessionId: {value: 'latest'}, scSession: {value: {id: 'historical'}},
            scSessionLoading: {value: false}, scObjectsLoading: {value: false}, scRefreshSession: () => refreshed++};
        vm.runInNewContext(app.slice(start, end), c);
        callback('diffs');
        expect(refreshed).toBe(0);
        callback('upload');
        expect(refreshed).toBe(1);
        c.scSession.value.id = 'latest';
        callback('upload');
        expect(refreshed).toBe(1);
    });
    it('routes launching through the normal dialog and blocks incomplete, busy, read-only and queued work', () => {
        const start = app.indexOf('        function scCatalogCanStart(');
        const end = app.indexOf('        function scBatchUrl(', start);
        const launches = [];
        const c = {scGroupStatus: () => null, scPairRowBusy: () => false,
            scOpenComparisonLaunchDialog: r => launches.push(r)};
        const flags = ['pcReadOnlySources', 'pcCatalogOpening', 'scSessionLoading', 'scObjectsLoading',
            'scPairingSaving', 'scPairingMatching', 'scDocumentGroupsBusy', 'scBatchRunning',
            'scProductionRunActive', 'scComparisonLaunchBusy', 'scComparisonLaunchAwaitingSheetMap'];
        flags.forEach(k => { c[k] = {value: false}; });
        vm.createContext(c);
        vm.runInContext(app.slice(start, end), c);
        const complete = row(0);
        c.scStartCatalogPair(complete);
        expect(launches).toEqual([complete]);
        c.scStartCatalogPair(row(1, {right: null}));
        for (const flag of flags) {
            c[flag].value = true;
            c.scStartCatalogPair(complete);
            c[flag].value = false;
        }
        c.scGroupStatus = () => ({label: 'Собирается сборка…'});
        c.scStartCatalogPair(complete);
        expect(launches).toHaveLength(1);
    });
});

describe('opening a saved result from an older session', () => {
    const navigation = app.slice(app.indexOf('async function pcEnsureResultSession('),
        app.indexOf('        // State of the V3 run of the opened pair'));
    const session = (extra = {}) => ({id: 'saved', stage_a_path: '/obj/stage_1', stage_b_path: '/obj/stage_2',
        pairs: [{id: 'saved-pair'}], documents: {}, ...extra});
    function harness(fetchImpl = async () => ({ok: true, json: async () => session()})) {
        const ref = value => ({value});
        const calls = [];
        const c = {pcContextEpoch: 1, scSessionRequestToken: 0, pcLoadToken: 0,
            currentObjectId: ref('obj'), scSession: ref({id: 'latest', pairs: []}),
            scSessionLoading: ref(false), scObjectsLoading: ref(false), scSessionError: ref(''),
            scSelectedObject: ref({stages: [{name: 'stage_1', path: '/obj/stage_1'}, {name: 'stage_2', path: '/obj/stage_2'}]}),
            pcCatalogFocus: ref(null), pcBridgeEnvelope: ref(null), pcBridgeUnavailable: ref(false), pcHistory: ref({}),
            scActivePair: ref(null), scPairData: ref(null), scMatchState: ref(null), scPairRowStates: {},
            scInvalidatePairOpen: () => calls.push('invalidate'), scResetProductionReview: () => {},
            scInitializeDocumentOrder: () => {}, scApplyDocumentDefaults: () => {},
            pcWaitFor: async ready => ready(), fetch: async url => {calls.push(url); return fetchImpl(url);},
            pcCatalogOpening: ref(false), pcCatalogError: ref(''), scPairLoading: ref(false), scTab: ref('upload'),
            pcEnvelopeValid: ref(false),
            scLoadObjects: async () => { c.scSession.value = {id: 'latest', pairs: []}; },
            scOpenPair: async pair => { c.scActivePair.value = pair; calls.push('pair:' + pair.id); },
            pcLoadBridge: async url => { calls.push(url); c.pcEnvelopeValid.value = true; },
            window: {ProjectComparisonCatalog: {model: () => 'test'}},
        };
        c.scPairs = {get value() { return c.scSession.value?.pairs || []; }};
        vm.createContext(c);
        vm.runInContext(navigation, c);
        return {c, calls};
    }

    it('opens the recorded session and pair after the default loader chooses a newer session', async () => {
        const {c, calls} = harness();
        await c.pcOpenCatalogEntry(entry({open: {available: true, object_id: 'obj', session_id: 'saved',
            pair_id: 'saved-pair', source_run_id: 'run', presentation_api: '/saved-presentation'}}));
        expect(c.pcCatalogError.value).toBe('');
        expect(c.scSession.value.id).toBe('saved');
        expect(c.scActivePair.value.id).toBe('saved-pair');
        expect(c.scTab.value).toBe('diffs');
        expect(c.pcCatalogFocus.value.source_run_id).toBe('run');
        expect(calls).toEqual(['/api/stage-comparison/sessions/saved', 'invalidate', 'pair:saved-pair', '/saved-presentation']);
    });

    it('reuses an already loaded result session', async () => {
        const {c, calls} = harness();
        c.scSession.value = session();
        await c.pcEnsureResultSession('saved', 'obj');
        expect(calls).toEqual([]);
    });

    it('rejects a foreign object or mismatched session without replacing the current session', async () => {
        for (const extra of [{stage_a_path: '/foreign/stage_1'}, {stage_b_path: '/foreign/stage_2'}, {id: 'other'}]) {
            const {c} = harness(async () => ({ok: true, json: async () => session(extra)}));
            await expect(c.pcEnsureResultSession('saved', 'obj')).rejects.toThrow('не принадлежит выбранному объекту');
            expect(c.scSession.value.id).toBe('latest');
            expect(c.scSessionLoading.value).toBe(false);
        }
    });

    it('reports a missing saved session immediately', async () => {
        const {c} = harness(async () => ({ok: false, json: async () => ({detail: 'Сессия не найдена'})}));
        await expect(c.pcEnsureResultSession('missing', 'obj')).rejects.toThrow('Сессия не найдена');
        expect(c.scSession.value.id).toBe('latest');
        expect(c.scSessionLoading.value).toBe(false);
    });

    it('does not install a late response after switching objects', async () => {
        let reply;
        const {c} = harness(() => new Promise(resolve => {reply = resolve;}));
        const pending = c.pcEnsureResultSession('saved', 'obj');
        await new Promise(resolve => setTimeout(resolve, 0));
        c.pcContextEpoch++; c.currentObjectId.value = 'other';
        reply({ok: true, json: async () => session()});
        await expect(pending).rejects.toThrow('Контекст сравнения изменился');
        expect(c.scSession.value.id).toBe('latest');
    });
});

describe('opening a saved result from the pair row', () => {
    const lookup = app.slice(app.indexOf('        function scRowCatalogEntry('),
        app.indexOf('        function scRowCatalogLabel('));
    const navigation = app.slice(app.indexOf('        async function scOpenPairRowResult('),
        app.indexOf('        async function scProcessPairRow('));
    const row = {left: {pdf_path: '/old.pdf'}, right: {pdf_path: '/assembly.pdf'}, pair: {id: 'p'}};
    function harness(entries) {
        const calls = [];
        const c = {pcCatalogEntries: {value: entries}, currentObjectId: {value: 'obj'},
            scSession: {value: {id: 's'}}, pcCatalogOpening: {value: false},
            scPairRowBusy: () => false,
            pcOpenCatalogEntry: async e => calls.push(['catalog', e.catalog_entry_id]),
            scOpenPairRow: async r => calls.push(['pair', r]),
        };
        vm.createContext(c);
        vm.runInContext(lookup + navigation, c);
        return {c, calls};
    }

    it('uses the catalog action for a saved pair, including catalog indexes without PDF paths', async () => {
        const saved = entry();
        const {c, calls} = harness([saved]);
        await c.scOpenPairRowResult(row);
        expect(calls).toEqual([['catalog', saved.catalog_entry_id]]);
        expect(html).toContain('@click="scOpenPairRowResult(row)">Открыть</button>');
    });

    it('finds a result for the same source versions in an older session', async () => {
        const saved = entry({open: {...entry().open, session_id: 'historical', pair_id: 'historical-pair'},
            documents: {old: {pdf_path: '/old.pdf'}, new: {pdf_path: '/assembly.pdf'}}});
        const {c, calls} = harness([saved]);
        await c.scOpenPairRowResult(row);
        expect(calls).toEqual([['catalog', saved.catalog_entry_id]]);
    });

    it('does not use results from a foreign object, another source version, or an unavailable run', async () => {
        for (const saved of [entry({object_id: 'foreign'}), entry({open: {...entry().open, available: false}}),
            entry({open: {...entry().open, session_id: 'historical'},
                documents: {old: {pdf_path: '/old.pdf'}, new: {pdf_path: '/different-version.pdf'}}})]) {
            const {c, calls} = harness([saved]);
            await c.scOpenPairRowResult(row);
            expect(calls).toEqual([['pair', row]]);
        }
    });

    it('keeps the pair workspace available when no analysis exists', async () => {
        const {c, calls} = harness([]);
        await c.scOpenPairRowResult(row);
        expect(calls).toEqual([['pair', row]]);
    });

    it('ignores repeated opening and incomplete rows', async () => {
        const {c, calls} = harness([entry()]);
        await c.scOpenPairRowResult({left: row.left});
        c.pcCatalogOpening.value = true;
        await c.scOpenPairRowResult(row);
        expect(calls).toEqual([]);
    });
});

describe('shared selection between source rows and catalog', () => {
    it('binds saved results and pending entries to their current source rows without mutating results', () => {
        const {sandbox} = load(async () => ({}));
        const saved = entry();
        const row = {left: {pdf_path: '/old'}, right: {pdf_path: '/new'}, pair: {id: 'p'}};
        const pending = {left: {pdf_path: '/old2'}, right: {pdf_path: '/new2'}};
        const historical = entry({catalog_entry_id: 'historical', open: {session_id: 'old', pair_id: 'old'}});
        const result = sandbox.ProjectComparisonCatalog.pairEntries([saved, historical], [row, pending], 'obj', '', 's');
        expect(result[0].selection_pair).toBe(row);
        expect(result[0].pending_pair).toBeUndefined();
        expect(result[1].selection_pair).toBe(pending);
        expect(result).toHaveLength(2);
        expect(saved.selection_pair).toBeUndefined();
    });

    it('uses the same parent selection for both directions and prevents selection while disabled', () => {
        const {definition} = load(async () => ({}));
        const selected = new Set();
        const row = {left: {}, right: {}};
        const props = {selectionDisabled: false, rowChecked: r => selected.has(r)};
        const events = [];
        const state = definition.setup(props, {emit: (event, r, checked) => {
            events.push(event);
            if (event === 'select-pair') checked ? selected.add(r) : selected.delete(r);
        }});
        state.selectPair({selection_pair: row}, true);
        expect(props.rowChecked(row)).toBe(true);
        selected.delete(row); // upper list unchecks the shared selection
        expect(props.rowChecked(row)).toBe(false);
        state.selectAll(true);
        props.selectionDisabled = true;
        state.selectPair({selection_pair: row}, true);
        state.selectAll(false);
        state.selectPair({}, true);
        expect(events).toEqual(['select-pair', 'select-all']);
        expect(html).toContain(':row-checked="scBatchRowChecked"');
        expect(html).toContain('@select-pair="scToggleBatchRow"');
        expect(html).toContain('@select-all="scToggleBatchAll"');
    });
});

it('shows stage and counter and exposes historical errors in the new column', () => {
    const {sandbox, definition} = load(async () => ({}));
    vm.runInNewContext(readFileSync(new URL('../static/js/project-change-view.js', import.meta.url), 'utf8'), sandbox);
    const progress = sandbox.ProjectComparisonCatalog.progressLabel;
    const state = {engine: 'projectchange_v3', status: 'REVIEW', consolidation:
        {status: 'RUNNING', live: true, calls_done: 4, calls_total: 48}};
    expect(progress(state)).toBe('07/7 · Сведение дублей · 4 из 48 обращений');
    expect(progress({...state, consolidation: {status: 'COMPLETED', stats: {calls: {PROVIDER_FAILED: 3}}}}))
        .toContain('Ошибка');
    expect(progress(null)).toBe('—');
    expect(definition.template.indexOf('<th>Ход выполнения</th>')).toBeGreaterThan(definition.template.indexOf('<th>Статус</th>'));
});

it('polls progress separately, rejects late object responses and stops on unmount', async () => {
    const requests = [];
    const {definition, mounted, watched, timers, unmounted, sandbox} = load(url => new Promise(resolve => requests.push({url, resolve})));
    sandbox.ProjectChangeView = {comparisonPipeline: state => ({title: state.message, stages: []})};
    const props = {objectId: '272', sessionId: 's'};
    const ui = definition.setup(props, {emit: () => {}});
    const first = mounted[1]();
    expect(requests[0].url).toBe('/api/project-comparison/catalog/progress?object_id=272');
    props.objectId = 'other'; watched[1].fn();
    requests[0].resolve({ok: true, json: async () => ({schema: 'project-comparison-progress/1', items: [
        {session_id: 's', pair_id: 'p', state: {message: 'stale'}}]})});
    await first;
    expect(ui.rowProgress(entry())).toBe('—');
    requests[1].resolve({ok: true, json: async () => ({schema: 'project-comparison-progress/1', items: [
        {session_id: 's', pair_id: 'p', state: {message: 'fresh'}}]})});
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(ui.rowProgress(entry())).toBe('fresh');
    expect(timers).toHaveLength(1);
    unmounted[0](); await timers[0]();
    expect(requests).toHaveLength(2);
});
