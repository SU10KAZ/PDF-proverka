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
            'Требуют проверки', 'Статус', 'Human Mapping', 'Диагностика'])
            expect(definition.template).toContain(column);
        expect(definition.template).toContain('entry.counts.projectchanges');
        expect(definition.template).toContain('entry.human_mapping.regions');
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
        state.openHumanMapping(state.entries.value[0]);
        expect(opened[0][0]).toBe('/human-mapping/?object=obj&comparison=p');
        state.openHumanMapping(entry({human_mapping: {available: false, url: null}}));
        expect(opened.length).toBe(1);
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
        expect(html).toContain('id="pc-catalog-focus"');
        expect(app).toContain('window.ProjectComparisonCatalog.register(app);');
    });

    it('opens a result through the normal pair workflow with a per-tab object', () => {
        const start = app.indexOf('async function pcOpenCatalogEntry');
        const body = app.slice(start, app.indexOf('\n        }\n', start));
        expect(body).toContain('storeObjectId(target.object_id)');
        expect(body).not.toContain('/objects/switch');
        expect(body).toContain('await scOpenPair(pair)');
        // Waits for the watcher's own session load instead of racing it with a second load.
        expect(body).toContain('await pcWaitFor(() => scSession.value?.id === target.session_id');
        expect(body).toContain('await nextTick();');
        expect(body).toContain("scTab.value = 'diffs'");
        expect(body).not.toMatch(/production\/run|scRunProduction|scStartProduction/);
        // The change list shows only the opened catalog run of that pair.
        expect(app).toContain('changes.filter(c => c.source_run_id === focus.source_run_id)');
    });
});
