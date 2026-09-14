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
        const {view}=mount(); expect(view.scope.value).toBe('pair');
        expect(view.visible.value).toHaveLength(10); expect(view.all.value).toHaveLength(10);
        expect(view.counts.value.review).toBe(10);
        expect(view.visible.value.every(c=>c.cipher==='ИОС2.1')).toBe(true);
        expect(view.visible.value[0].display_id).toBe('PC-001');
    });
    it('canonical pair change replaces rows, clears stale filters and expansion', async () => {
        const {props,view}=mount(); view.filters.source='GRAPHIC';view.toggle(changes[0]);
        props.selectedPairId=heat;await context.Vue.nextTick();
        expect(view.visible.value).toHaveLength(15);
        expect(view.visible.value.every(c=>c.cipher==='ИОС4.1')).toBe(true);
        expect(view.expandedId.value).toBe('');expect(view.filters.source).toBe('');
    });
    it('all pairs exposes 73 events; returning to current pair preserves selection', async () => {
        const {view}=mount();view.scope.value='all';await context.Vue.nextTick();
        expect(view.visible.value).toHaveLength(73);
        expect(new Set(view.visible.value.map(c=>c.display_id)).size).toBe(73);
        view.scope.value='pair';await context.Vue.nextTick();expect(view.visible.value).toHaveLength(10);
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
    it.each(['cipher','system','type','status','source'])('filters %s within pair and retains the denominator', key => {
        const {view}=mount(),c=view.all.value.find(c=>c.engineering_system)||view.all.value[0];
        const values={cipher:`${c.cipher} · ${c.discipline}`,system:c.engineering_system,type:c.change_type,status:c.status,source:c.evidence[0].source_type};
        view.filters[key]=values[key];
        expect(view.visible.value).toEqual(V.filter(V.inPair(changes,water),{[key]:values[key]}));
        expect(view.all.value).toHaveLength(10);view.filters[key]='no-match';expect(view.visible.value).toEqual([]);
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
const selectStart=appSource.indexOf('async function pcSelectPair(');
const selectCode=appSource.slice(selectStart,appSource.indexOf('let pcNavigationToken',selectStart));
function selectionHarness(){
    let reply;
    const calls=[],pair=pairs.find(p=>p.id===water),ctx={scPairs:{value:pairs},pcUiEnabled:{value:true},scPairLoading:{value:false},
        pcContextEpoch:0,pcError:{value:''},scSessionError:{value:''},scTab:{value:'diffs'},
        scOpenPair:p=>{calls.push(p);return new Promise(r=>{reply=r;});}};
    vm.createContext(ctx);vm.runInContext(selectCode,ctx);
    return {ctx,calls,pair,reply:value=>reply(value)};
}
describe('Page 3 selects through existing Page 1/2 state',()=>{
    it('passes the canonical registry pair to scOpenPair and returns to Page 3',async()=>{
        const h=selectionHarness(),pending=h.ctx.pcSelectPair(water);
        expect(h.calls).toEqual([h.pair]);h.ctx.scTab.value='links';h.reply({pair:h.pair});await pending;
        expect(h.ctx.scTab.value).toBe('diffs');
    });
    it('rejects unknown pair, legacy context and concurrent pair open',async()=>{
        const h=selectionHarness();await h.ctx.pcSelectPair('unknown');
        h.ctx.pcUiEnabled.value=false;await h.ctx.pcSelectPair(water);
        h.ctx.pcUiEnabled.value=true;h.ctx.scPairLoading.value=true;await h.ctx.pcSelectPair(water);
        expect(h.calls).toEqual([]);
    });
    it('reports a read failure without changing selection or hiding Page 3',async()=>{
        const h=selectionHarness(),pending=h.ctx.pcSelectPair(water);h.ctx.scSessionError.value='Недоступно';h.reply(null);await pending;
        expect(h.ctx.pcError.value).toBe('Недоступно');expect(h.ctx.scTab.value).toBe('diffs');
    });
    it('does not navigate on a stale response after object change',async()=>{
        const h=selectionHarness(),pending=h.ctx.pcSelectPair(water);h.ctx.pcContextEpoch++;h.ctx.scTab.value='upload';h.reply({pair:h.pair});await pending;
        expect(h.ctx.scTab.value).toBe('upload');
    });
});
