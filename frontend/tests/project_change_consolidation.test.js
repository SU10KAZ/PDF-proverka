import {afterEach, describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import vm from 'node:vm';
const require = createRequire(import.meta.url);
const M = require('../static/js/project-change-consolidation.js');
const timers = new Set();
afterEach(() => { for (const timer of timers) clearTimeout(timer); timers.clear(); });

const evidence = (id, side, page) => ({id, side, page, pair_id: 'pair-1', source_type: 'TEXT', region: {x: 0.1, y: 0.1, width: 0.5, height: 0.2, units: 'normalized'},
    document: {id: 'doc', label: 'ИОС', version: 'v002', pdf_path: `/p/${side}.pdf`}, image_url: `/crop/${id}`, quote: 'q'});
const entry = (extra = {}) => ({source_run_id: 'src1', consolidator_run_id: 'c1', source_kind: 'PRODUCTION_RUN',
    source_is_current_run: true, source_result_sha256: 'a'.repeat(64), shadow_result_sha256: 'b'.repeat(64),
    model: 'claude-opus-5', reasoning: 'xhigh', completed_at: '2026-09-24T10:00:00+00:00', ...extra});
const view = (extra = {}) => ({schema: 'projectchange-consolidated-view/1', source_run_id: 'src1', consolidator_run_id: 'c1',
    source_result_sha256: 'a'.repeat(64), shadow_result_sha256: 'b'.repeat(64),
    original: [{id: 'PC-1', title: 'A', evidence: [evidence('e1', 'OLD', 3), evidence('e2', 'NEW', 9)]},
               {id: 'PC-2', title: 'B', evidence: [evidence('e3', 'OLD', 4)]}],
    consolidated: [{kind: 'CONSOLIDATED', id: 'pcc:1', channel: 'ENGINEERING_CHANGE', title: 'A+B',
        lineage: {members: [{id: 'PC-1'}, {id: 'PC-2'}]}, evidence: [evidence('e1', 'OLD', 3), evidence('e2', 'NEW', 9)], conflicts: []}],
    ...extra});

function harness(responses) {
    const context = vm.createContext({console, clearTimeout,
        setTimeout: (callback, delay) => { const timer = setTimeout(callback, delay); timers.add(timer); return timer; },
        fetch: async url => { const body = responses(url); return {ok: body !== null, json: async () => body}; }});
    vm.runInContext(readFileSync(new URL('../static/js/vue.global.prod.js', import.meta.url), 'utf8'), context);
    vm.runInContext(readFileSync(new URL('../static/js/project-change-consolidation.js', import.meta.url), 'utf8'), context);
    let component;
    context.ProjectChangeConsolidation.register({component: (_, c) => { component = c; }});
    const props = context.Vue.reactive({sessionId: 's1', pairId: 'pair-1'});
    const events = [];
    const vm_ = component.setup(props, {emit: (...args) => events.push(args)});
    return {context, component, props, events, vm: vm_};
}
const settle = () => new Promise(r => setTimeout(r, 0));

describe('Consolidator «Исходные | Итоговые» model', () => {
    it('limits parameter references to its source cards, separates sides and deduplicates PDF pages', () => {
        const original = [
            {id: 'a', evidence: [evidence('a1', 'OLD', 12), evidence('a2', 'OLD', 3), evidence('a3', 'NEW', 8)]},
            {id: 'b', evidence: [evidence('b1', 'OLD', 3)]},
            {id: 'unrelated', evidence: [evidence('c1', 'OLD', 99)]},
        ];
        expect(M.parameterSources({kind: 'CONSOLIDATED'}, {values: [{card_id: 'a'}, {card_id: 'b'}, {card_id: 'a'}]}, original))
            .toEqual({OLD: [{cipher: 'ИОС', sourceType: 'TEXT', pages: '3, 12'}], NEW: [{cipher: 'ИОС', sourceType: 'TEXT', pages: '8'}]});
        expect(M.parameterSources({kind: 'CONSOLIDATED', evidence: original[2].evidence}, {values: [{card_id: 'missing'}]}, original))
            .toEqual({OLD: [], NEW: []});
    });
    it('uses original source PDF numbering and ciphers for assembly evidence', () => {
        const e = {...evidence('e1', 'OLD', 208), origin: {
            source: {document_code: 'ВК-1', version_id: 'v1'}, page: {source_page: 5}, assembly_page: 208}};
        expect(M.parameterSources({kind: 'SOURCE_CARD', evidence: [e, evidence('e2', 'NEW', 7)]}, {}, []))
            .toEqual({OLD: [{cipher: 'ВК-1', sourceType: 'TEXT', pages: '5'}], NEW: [{cipher: 'ИОС', sourceType: 'TEXT', pages: '7'}]});
        expect(M.parameterSources({evidence: [{side: 'OLD', page: 0}]}, {}, []))
            .toEqual({OLD: [{cipher: '—', sourceType: '—', pages: '—'}], NEW: []});
    });
    it('keeps each evidence type next to its own PDF pages on each side', () => {
        const card = {kind: 'SOURCE_CARD', evidence: [
            {...evidence('a', 'OLD', 3), source_type: 'TABLE'},
            {...evidence('b', 'OLD', 3), source_type: 'TABLE'},
            evidence('c', 'OLD', 4),
            {...evidence('d', 'NEW', 23), source_type: 'GRAPHIC'},
            {...evidence('e', 'NEW', 30), source_type: 'GRAPHIC'},
            {...evidence('f', 'NEW', 31), source_type: 'TABLE'},
        ]};
        expect(M.parameterSources(card, {}, [])).toEqual({
            OLD: [{cipher: 'ИОС', sourceType: 'TABLE', pages: '3'}, {cipher: 'ИОС', sourceType: 'TEXT', pages: '4'}],
            NEW: [{cipher: 'ИОС', sourceType: 'GRAPHIC', pages: '23, 30'}, {cipher: 'ИОС', sourceType: 'TABLE', pages: '31'}],
        });
    });
    it('prefers the consolidation of the displayed run, else the latest', () => {
        expect(M.pick([entry({source_is_current_run: false, completed_at: '2026-09-25'}), entry()]).completed_at).toBe('2026-09-24T10:00:00+00:00');
        expect(M.pick([entry({source_is_current_run: false, consolidator_run_id: 'old', completed_at: '2026-09-01'}),
            entry({source_is_current_run: false, consolidator_run_id: 'new', completed_at: '2026-09-02'})]).consolidator_run_id).toBe('new');
        expect(M.pick([])).toBe(null);
    });
    it('summarises and navigates to both sides of one card', () => {
        const s = M.summary(view());
        expect(s).toMatchObject({original: 2, consolidated: 1, merged: 1, absorbed: 2});
        const t = M.destination(view().consolidated[0]);
        expect(t.OLD.id).toBe('e1'); expect(t.NEW.id).toBe('e2'); expect(t.change_id).toBe('pcc:1');
        expect(M.destination({id: 'x', evidence: [{id: 'z', side: 'OLD'}]})).toBe(null);
    });
});

describe('component', () => {
    it('compiles its template without errors', () => {
        const {context, component} = harness(() => null);
        // Minimal entity decoder of the browser compiler (text and attribute values pass through).
        context.document = {createElement: () => ({
            set innerHTML(v) { this.v = v; }, get textContent() { return this.v; },
            get children() { const m = /^<div foo="([\s\S]*)">$/.exec(this.v);
                return [{getAttribute: () => (m ? m[1] : '').replace(/&quot;/g, '"')}]; }})};
        const errors = [];
        const render = context.Vue.compile(component.template, {onError: e => errors.push(String(e))});
        expect(errors).toEqual([]); expect(typeof render).toBe('function');
    });
    it('shows nothing and keeps the ordinary list when there is no consolidation', async () => {
        const {vm: v, events} = harness(() => ({schema: 'projectchange-consolidations/1', consolidations: []}));
        await settle();
        expect(v.list.value).toEqual([]); expect(v.ownList.value).toBeFalsy();
        expect(events.at(-1)).toEqual(['panel', false]);
    });
    it('Итоговые load only a view bound to the listed source and shadow sha256', async () => {
        const {vm: v, events} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry()]} : view());
        await settle();
        expect(v.cards.value[0].kind).toBe('CONSOLIDATED');
        expect(events.at(-1)).toEqual(['panel', true]);
        expect(v.cards.value.map(c => c.id)).toEqual(['pcc:1']);
        expect(v.show).toBeUndefined();
    });
    it('refuses a view whose sha256 does not match the listing', async () => {
        const {vm: v} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry()]} : view({shadow_result_sha256: 'c'.repeat(64)}));
        await settle();
        expect(v.cards.value).toEqual([]); expect(v.error.value).toContain('недоступен');
        expect(v.ownList.value).toBe(true);
    });
    it('automatically shows final cards even when the source is not the displayed run', async () => {
        const {vm: v} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry({source_is_current_run: false})]} : view());
        await settle();
        expect(v.ownList.value).toBe(true);
        expect(v.cards.value.map(c => c.id)).toEqual(['pcc:1']);
    });
    it('loads a selected final result automatically and discards the previous result arriving late', async () => {
        let finishFirst;
        const pending = new Promise(resolve => { finishFirst = resolve; });
        const {vm: v, events} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry(), entry({consolidator_run_id: 'c2'})]}
            : url.endsWith('/src1/c1') ? pending
                : url.endsWith('/src1/c2') ? view({consolidator_run_id: 'c2',
                    consolidated: [{kind: 'CONSOLIDATED', id: 'new-final'}]}) : null);
        expect(v.ownList.value).toBe(true);
        await settle();
        expect(v.loading.value).toBe(true);
        v.selectedKey.value = 'src1/c2';
        await settle();
        expect(v.cards.value.map(c => c.id)).toEqual(['new-final']);
        finishFirst(view());
        await settle();
        expect(v.cards.value.map(c => c.id)).toEqual(['new-final']);
        expect(v.loading.value).toBe(false);
        expect(events.every(e => e[0] !== 'panel' || e[1] === true)).toBe(true);
    });
    it('offers the Excel report of the selected consolidation and shows the analysis status', async () => {
        const {vm: v} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry()]}
            : url.endsWith('/it-analysis') ? {schema: 'projectchange-it-analysis-status/1', available: true, rows: 46,
                filled: 46, uploaded_at: '2026-10-08T12:00:00+00:00', warnings: []} : view());
        await settle(); await settle();
        expect(v.reportBase.value).toBe('/api/stage-comparison/sessions/s1/pairs/pair-1/consolidated/src1/c1');
        expect(v.analysis.value).toMatchObject({available: true, rows: 46});
    });
    it('downloads the classic report without starting a model, even during an AI run', async () => {
        const {context, vm: v} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry()]} : {status: 'NONE', available: false});
        await settle();
        const requests = [];
        context.fetch = async (...args) => { requests.push(args); return {ok: true, json: async () => ({})}; };
        context.location = {href: ''};
        for (const status of ['NONE', 'RUNNING', 'CANCELLED']) {
            v.modelRun.value = {status};
            context.location.href = '';
            v.download();
            expect(context.location.href).toBe('/api/stage-comparison/sessions/s1/pairs/pair-1/consolidated/src1/c1/report.xlsx');
        }
        expect(requests).toEqual([]);
    });
    it('uploads the filled analysis and lists legend violations', async () => {
        const calls = [];
        const {context, vm: v} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry()]} : null);
        await settle();
        context.FormData = class { append(k, f) { this.k = k; this.f = f; } };
        context.fetch = async (url, init) => { calls.push([url, init?.method]);
            return {ok: false, json: async () => ({detail: {message: 'Разбор не принят', errors: ['строка 3: корзина']}})}; };
        const input = {files: [{name: 'x.xlsx'}], value: 'x.xlsx'};
        await v.uploadAnalysis({target: input});
        expect(calls).toEqual([['/api/stage-comparison/sessions/s1/pairs/pair-1/consolidated/src1/c1/it-analysis', 'POST']]);
        expect(v.uploadMessage.value).toBe('Разбор не принят'); expect(v.uploadErrors.value).toEqual(['строка 3: корзина']);
        expect(input.value).toBe('');
    });
    it('shows a network failure and permits another start', async () => {
        const {context, vm: v} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry()]} : {status: 'NONE', available: false});
        await settle();
        context.fetch = async () => { throw new Error('offline'); };
        await v.makeReport();
        expect(v.uploadMessage.value).toContain('ошибка связи');
        expect(v.starting.value).toBe(false);
    });
    it('does not attach a late model start to another consolidation', async () => {
        const {context, vm: v} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry(), entry({consolidator_run_id: 'c2'})]} : {status: 'NONE', available: false});
        await settle();
        let finish;
        context.fetch = (url, options) => options?.method === 'POST'
            ? new Promise(resolve => { finish = resolve; })
            : Promise.resolve({ok: true, json: async () => ({status: 'NONE', available: false})});
        const pending = v.makeReport();
        expect(v.starting.value).toBe(true);
        v.selectedKey.value = 'src1/c2';
        await settle();
        finish({ok: true, json: async () => ({status: 'RUNNING', run_id: 'old'})});
        await pending;
        expect(v.modelRun.value?.run_id).not.toBe('old');
        expect(v.starting.value).toBe(false);
    });
    it('opens evidence through the ordinary evidence navigation', async () => {
        const {vm: v, events} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry()]} : view());
        await settle();
        v.open(v.cards.value[0], v.cards.value[0].evidence[1]);
        const [name, target] = events.at(-1);
        expect(name).toBe('open-evidence'); expect(target.NEW.id).toBe('e2'); expect(target.OLD.id).toBe('e1');
    });
});
