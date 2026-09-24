import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import vm from 'node:vm';
const require = createRequire(import.meta.url);
const M = require('../static/js/project-change-consolidation.js');

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
    const context = vm.createContext({console, setTimeout,
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
        expect(events.every(e => e[0] !== 'panel' || e[1] === false)).toBe(true);
    });
    it('Итоговые load only a view bound to the listed source and shadow sha256', async () => {
        const {vm: v, events} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry()]} : view());
        await settle();
        await v.show('consolidated');
        expect(v.mode.value).toBe('consolidated'); expect(v.cards.value[0].kind).toBe('CONSOLIDATED');
        expect(events.at(-1)).toEqual(['panel', true]);
        await v.show('original');  // source is the displayed run: the ordinary list is the original view
        expect(v.ownList.value).toBeFalsy(); expect(events.at(-1)).toEqual(['panel', false]);
    });
    it('refuses a view whose sha256 does not match the listing', async () => {
        const {vm: v} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry()]} : view({shadow_result_sha256: 'c'.repeat(64)}));
        await settle();
        await v.show('consolidated');
        expect(v.mode.value).toBe(''); expect(v.error.value).toContain('недоступен');
    });
    it('for a source that is not the displayed run, Исходные are that source run’s own cards', async () => {
        const {vm: v} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry({source_is_current_run: false})]} : view());
        await settle();
        await v.show('original');
        expect(v.ownList.value).toBe(true);
        expect(v.cards.value.map(c => c.id)).toEqual(['PC-1', 'PC-2']);
        expect(v.cards.value.every(c => c.kind === 'SOURCE_CARD')).toBe(true);
    });
    it('opens evidence through the ordinary evidence navigation', async () => {
        const {vm: v, events} = harness(url => url.endsWith('/consolidated')
            ? {schema: 'projectchange-consolidations/1', consolidations: [entry()]} : view());
        await settle();
        await v.show('consolidated');
        v.open(v.cards.value[0], v.cards.value[0].evidence[1]);
        const [name, target] = events.at(-1);
        expect(name).toBe('open-evidence'); expect(target.NEW.id).toBe('e2'); expect(target.OLD.id).toBe('e1');
    });
});
