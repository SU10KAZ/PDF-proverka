import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import vm from 'node:vm';
const require = createRequire(import.meta.url);
const V = require('../static/js/project-change-view.js');
// Release-bundled presentation only. Never load research fixtures.
const snapshot = JSON.parse(readFileSync(new URL('../../backend/app/data/project_change_preview_snapshot/presentation.json', import.meta.url)));
const envelope = {...snapshot.envelope, object_id: V.OBJECT};
const changes = V.fromEnvelope(envelope, V.OBJECT), pairs = envelope.viewer_session.pairs;
const water = changes.find(c => c.cipher === 'ИОС2.1').pair_ids[0];
const heat = changes.find(c => c.cipher === 'ИОС4.1').pair_ids[0];
const context = vm.createContext({ProjectChangeView: V});
vm.runInContext(readFileSync(new URL('../static/js/vue.global.prod.js', import.meta.url), 'utf8'), context);
vm.runInContext(readFileSync(new URL('../static/js/project-change-ui.js', import.meta.url), 'utf8'), context);
let component;
context.ProjectChangeUI.register({component: (_, c) => { component = c; }});
function mount(extra = {}) {
    const props = context.Vue.reactive({changes, pairs, selectedPairId: water, report: false, readonly: true, ...extra});
    const events = [], view = component.setup(props, {emit: (...args) => events.push(args)});
    return {props, view, events};
}
describe('frozen ProjectChange pair binding', () => {
    it('validates all 283 evidence entries in all 73 events against document paths and versions', () => {
        expect(changes).toHaveLength(73);
        expect(changes.every(c => !c.pair_binding_error && c.pair_ids.length === 1)).toBe(true);
        expect(new Set(changes.flatMap(c => c.pair_ids)).size).toBe(9);
        expect(changes.reduce((n,c) => n+c.evidence.length,0)).toBe(283);
    });
    it('checks later evidence, failing closed on unknown pair, wrong version/path or missing metadata', () => {
        const c = changes.find(c => c.evidence.length > 1);
        for (const wrong of [null, {...c.evidence[1],pair_id:'foreign'},
            {...c.evidence[1],document:{...c.evidence[1].document,version:'v003'}},
            {...c.evidence[1],document:{...c.evidence[1].document,pdf_path:'same-name-different-document'}}]) {
            expect(V.pairBinding([c.evidence[0],wrong],pairs)).toEqual({pair_ids:[],pair_binding_error:true});
        }
        expect(V.pairBinding([],pairs).pair_binding_error).toBe(true);
        expect(V.pairBinding(c.evidence,[]).pair_binding_error).toBe(true);
    });
    it('retains all pairs for a multi-pair event without duplicating rows', () => {
        const a=changes.find(c=>c.pair_ids.includes(water)), b=changes.find(c=>c.pair_ids.includes(heat));
        const multi={...a,...V.pairBinding([...a.evidence,...b.evidence],pairs)};
        expect(multi.pair_ids).toHaveLength(2);
        expect(V.inPair([multi],water)).toEqual([multi]); expect(V.inPair([multi],heat)).toEqual([multi]);
    });
});
describe('pair-scoped table component', () => {
    it('defaults to selected pair with pair-relative totals and stable display IDs', () => {
        const {view}=mount();
        expect(view.visible.value).toHaveLength(10); expect(view.all.value).toHaveLength(10);
        expect(view.counts.value.review).toBe(10);
        expect(view.visible.value.every(c=>c.cipher==='ИОС2.1')).toBe(true);
        expect(view.visible.value[0].display_id).toBe('PC-001');
    });
    it('canonical pair change replaces rows and clears expansion', async () => {
        const {props,view}=mount(); view.toggle(changes[0]);
        props.selectedPairId=heat;await context.Vue.nextTick();
        expect(view.visible.value).toHaveLength(15);
        expect(view.visible.value.every(c=>c.cipher==='ИОС4.1')).toBe(true);
        expect(view.expandedId.value).toBe('');
    });
    it('has no pair switch, all-pairs mode or filter state', () => {
        const {view}=mount();
        expect(view.scope).toBeUndefined(); expect(view.filters).toBeUndefined();
        expect(component.emits).not.toContain('select-pair');
        for(const control of ['pc-pair-toolbar', 'pc-scope-switch', 'pc-filters', 'Все пары объекта']) {
            expect(component.template).not.toContain(control);
        }
        expect(component.emits).toContain('open-upload');
    });
    it('direct entry and stale selection never pick a random pair', () => {
        for(const selectedPairId of ['', 'stale']) {
            const {view}=mount({selectedPairId});expect(view.needsPair.value).toBe(true);expect(view.visible.value).toEqual([]);
        }
    });
    it('known pair without changes stays empty', () => {
        const pair=pairs.find(p=>!changes.some(c=>c.pair_ids.includes(p.id)));
        const {view}=mount({selectedPairId:pair.id});expect(view.needsPair.value).toBe(false);expect(view.visible.value).toEqual([]);
    });
    it('all four counters use the current pair, including one change and zero changes', () => {
        for (const pair of pairs) {
            const {view}=mount({selectedPairId:pair.id});
            const expected=V.inPair(changes,pair.id);
            expect(view.visible.value).toEqual(expected);
            expect(view.counts.value).toEqual(V.summary(expected));
        }
    });
    it('removing the current pair clears the table instead of selecting another', async () => {
        const {props,view}=mount();
        props.pairs=pairs.filter(p=>p.id!==water);await context.Vue.nextTick();
        expect(view.needsPair.value).toBe(true);expect(view.visible.value).toEqual([]);
    });
    it('expansion starts closed, toggles, and opens only one event', () => {
        const {view}=mount();expect(view.expandedId.value).toBe('');view.toggle(changes[0]);expect(view.expandedId.value).toBe(changes[0].id);
        view.toggle(changes[1]);expect(view.expandedId.value).toBe(changes[1].id);view.toggle(changes[1]);expect(view.expandedId.value).toBe('');
    });
    it('groups TEXT/TABLE/GRAPHIC without duplicate badges', () => {
        const {view}=mount(),c={evidence:changes.flatMap(c=>c.evidence)};
        expect(view.presentSources(c)).toEqual(['TEXT','TABLE','GRAPHIC']);
    });
    it('preserves full states; compacts only presentation and never invents OLD absence', () => {
        const c=changes.find(c=>c.old_state.length>140);
        expect(c.old_state).toBe(envelope.items.find(i=>i.id===c.id).old_state);
        expect(V.compactText(c.old_state)).toHaveLength(140);expect(V.compactText('')).toBe('');expect(V.displaySystem('UNKNOWN')).toBe('');
        expect(changes.filter(c=>!c.old_state)).toHaveLength(59);
    });
    it('enlarge uses selected evidence; PDF event preserves exact page/version/region', async () => {
        const {view,events}=mount(),c=changes.find(c=>c.evidence.some(e=>e.region)),e=c.evidence.find(e=>e.region);let shown=false;
        view.imageDialog.value={showModal:()=>{shown=true;}};await view.enlarge(e);
        expect(shown).toBe(true);expect(view.selectedImage.value.id).toBe(e.id);
        view.open(c,e);expect(events[0]).toEqual(['open-evidence',V.destination(c,e)]);
    });
    it('Page 4 stays empty, including 14 research PROVEN', () => {
        expect(changes.filter(c=>c.research_status==='PROVEN')).toHaveLength(14);
        expect(changes.every(c=>c.status==='REVIEW')).toBe(true);expect(mount({report:true}).view.all.value).toEqual([]);
    });
    it('read-only envelope cannot import a human confirmation', () => {
        const c=envelope.items[0],env={...envelope,items:[{...c,decision_state:'ACTIVE',effective_decision:{decision:'CONFIRM',decision_key:c.decision_key,binding_signature:c.binding_signature,candidate_version:c.candidate_version}}]};
        expect(V.fromEnvelope(env,V.OBJECT)[0].status).toBe('REVIEW');
    });
});

const appSource=readFileSync(new URL('../static/js/app.js',import.meta.url),'utf8');
const navigationStart=appSource.indexOf('let pcNavigationToken =');
const navigationCode=appSource.slice(navigationStart,appSource.indexOf('const scTextDifferenceFilterOptions',navigationStart));
describe('evidence navigation retains the pair opened on Page 1',()=>{
    it.each(['', heat])('rejects a target in another pair when active pair is %s',async activeId=>{
        const c=changes.find(c=>c.pair_ids.includes(water)),target=V.destination(c);
        const ctx={scPairs:{value:pairs},pcUiEnabled:{value:true},scSession:{value:{id:'preview'}},
            scActivePair:{value:pairs.find(p=>p.id===activeId)||null},scTab:{value:'diffs'},pcError:{value:''}};
        vm.createContext(ctx);vm.runInContext(navigationCode,ctx);
        await ctx.pcOpenEvidence(target);
        expect(ctx.pcError.value).toBe('Сначала откройте пару документов на вкладке «Загрузка документации».');
        expect(ctx.scActivePair.value?.id||'').toBe(activeId);expect(ctx.scTab.value).toBe('diffs');
    });
    it('cannot switch pair through a removed Page 3 handler',()=>{
        expect(appSource).not.toContain('pcSelectPair');
        const html=readFileSync(new URL('../index.html',import.meta.url),'utf8');
        expect(html).toContain(':selected-pair-id="scActivePair?.id || \'\'"');
        expect(html).toContain('@open-upload="scTab = \'upload\'"');
        expect(html).not.toContain('RESEARCH / PREVIEW');
        expect(html).not.toContain('Доступен просмотр исходных PDF.');
    });
});
