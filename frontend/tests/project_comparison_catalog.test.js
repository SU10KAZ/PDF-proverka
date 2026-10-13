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
    const opened = [];
    const sandbox = {Vue: {ref, computed, onMounted: fn => mounted.push(fn)}, fetch: fetchImpl,
        open: (...args) => opened.push(args), localStorage: {getItem: () => null, setItem: () => {}}};
    sandbox.globalThis = sandbox;
    vm.runInNewContext(source, sandbox);
    let definition;
    sandbox.ProjectComparisonCatalog.register({component: (name, def) => { definition = {name, ...def}; }});
    return {sandbox, definition, mounted, opened};
}

describe('Проверенные сравнения — catalog component', () => {
    it('registers a read-only catalog component with the recommended columns', () => {
        const {definition} = load(async () => ({}));
        expect(definition.name).toBe('project-comparison-catalog');
        expect(definition.emits).toEqual(['open']);
        for (const column of ['Проект / раздел', 'OLD → NEW', 'Модель', 'V3', 'Дата', 'Изменения',
            'Требуют проверки', 'Статус', 'Смысловые блоки в шаге 2', 'Диагностика'])
            expect(definition.template).toContain(column);
        expect(definition.template).toContain('entry.counts.projectchanges');
        expect(definition.template).not.toContain('Human Mapping');
        // No analysis controls in the catalog.
        expect(source).not.toMatch(/production\/run|Запустить анализ|method:\s*'POST'/);
    });

    it('loads entries from the catalog API and emits the entry to open', async () => {
        const calls = [];
        const payload = {schema: 'project-comparison-catalog/1', entries: [entry()]};
        const {definition, mounted, opened} = load(async url => {
            calls.push(url);
            return {ok: true, json: async () => payload};
        });
        const emitted = [];
        const state = definition.setup({activeEntryId: '', opening: false, error: ''},
            {emit: (...args) => emitted.push(args)});
        await mounted[0]();
        expect(calls).toEqual(['/api/project-comparison/catalog']);
        expect(state.entries.value.map(e => e.title)).toEqual(['ШИФР-НОВЫЙ']);
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
        const state = definition.setup({objectId: 'obj'}, {emit: () => {}});
        await mounted[0]();
        expect(state.entries.value.map(e => e.title)).toEqual(['СВОЙ']);
        const empty = definition.setup({objectId: 'none'}, {emit: () => {}});
        await mounted[1]();
        expect(empty.entries.value).toEqual([]);
        expect(definition.template).toContain('В этом объекте завершённых результатов пока нет.');
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
