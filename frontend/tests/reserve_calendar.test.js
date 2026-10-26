import { describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { randomUUID } from 'node:crypto';

const source = readFileSync(new URL('../static/js/reserve-calendar.js', import.meta.url), 'utf8');
function harness(responses = [], browserCrypto = {randomUUID}) {
    const components = {}, emit = vi.fn();
    const fetch = vi.fn(async () => {
        const value = responses.shift();
        if (value instanceof Error) throw value;
        return {ok: !value?.detail, status: value?.detail ? 409 : 200, json: async () => value};
    });
    const context = {Vue: {ref: value => ({value}), computed: get => ({get value() {return get();}}),
        watch: () => {}, onMounted: () => {}, onBeforeUnmount: () => {}, nextTick: async () => {}},
        fetch, crypto:browserCrypto, window:{innerWidth:1200,innerHeight:800}, Date, Map, Set, Object, JSON, Number,
        setInterval, clearInterval};
    vm.runInNewContext(source, context);
    context.window.ReserveCalendar.register({component:(name, value) => {components[name] = value;}});
    return { ...context.window.ReserveCalendar.helpers, state: components['reserve-calendar'].setup({active:true},{emit}), fetch, emit };
}
const data = () => ({today:'2026-10-26',revision:0,can_edit:true, calendar:{},totals:{},available_projects:[],
    engineers:[{id:'a',name:'A',direction:'AR'},{id:'b',name:'B',direction:'AR'}],
    tasks:[{id:'1',engineer_id:'a',position:1,state:'ready',title:'One',project_id:'AR/Full',version_id:'v002',object_id:'o'},
        {id:'2',engineer_id:'a',position:2,state:'preparation',title:'Two'},
        {id:'3',engineer_id:'b',position:1,state:'ready',title:'Three'}]});

const catalogData = () => ({...data(), available_projects:[
    {object_id:'o2',object_name:'272. Балчуг Эстейт',project_id:'AR/P2',version_id:'v002',title:'АР2',ready:true},
    {object_id:'o1',object_name:'213. King & Sons',project_id:'AR/P1',version_id:'v001',title:'АР1',ready:true},
    {object_id:'o1',object_name:'213. King & Sons',project_id:'KJ/P3',version_id:'v001',title:'КЖ',ready:false},
]});

describe('reserve calendar user actions', () => {
    it('shows four project characters followed by two dots', () => {
        const h = harness();
        expect(h.state.compactProject('13АВ-РД-ВК1-К2')).toBe('13АВ..');
        expect(h.state.compactProject('АР2')).toBe('АР2');
    });
    it('submits operations on an HTTP origin where crypto.randomUUID is unavailable', async () => {
        const browserCrypto = {getRandomValues(bytes) { for (let i = 0; i < bytes.length; i++) bytes[i] = i; return bytes; }};
        const h = harness([data(), {...data(), revision:1}], browserCrypto);
        await h.state.load();
        await h.state.operate('move', {task_ids:['1'], engineer_id:'b'});
        const sent = JSON.parse(h.fetch.mock.calls[1][1].body);
        expect(sent.operation_id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
        expect(h.state.data.value.revision).toBe(1);
    });
    it('splits pasted lines without clipping names and detects NFC/spacing/case duplicates across fields', () => {
        const h = harness();
        expect(h.lines(' \n  A full title  \r\n\nSecond\u2028Third')).toEqual(['A full title','Second','Third']);
        const errors = h.duplicateFields(data(), {a:'И\u0306   Том',b:'й том'});
        expect(Object.keys(errors)).toEqual(['b','a']);
        const sameTextDifferentVersion = h.duplicateFields(data(),{a:'One'});
        expect(sameTextDifferentVersion).toEqual({});
    });
    it('Ctrl/Cmd select only; a plain click opens the precise object/project/version', async () => {
        const h = harness([data()]); await h.state.load();
        const task = h.state.data.value.tasks[0];
        h.state.clickTask(task,{ctrlKey:true});
        expect(h.state.selected.value).toEqual(['1']); expect(h.emit).not.toHaveBeenCalled();
        h.state.clickTask(task,{metaKey:true}); expect(h.state.selected.value).toEqual([]);
        h.state.clickTask(task,{});
        expect(h.emit).toHaveBeenCalledWith('open-project',expect.objectContaining({project_id:'AR/Full',version_id:'v002',object_id:'o'}));
    });
    it('orders multi-row selections by current display order, not click order', () => {
        const h = harness();
        expect(h.orderedSelection(data(),['3','2','1']).map(t=>t.id)).toEqual(['1','2','3']);
    });
    it('uses an insertion marker and suppresses project navigation after drop', async () => {
        const h = harness([data(), {...data(),revision:1}]); await h.state.load();
        const s=h.state; s.selected.value=['3','1'];
        const transfer={effectAllowed:'',setData:vi.fn()};
        s.dragStart(s.data.value.tasks[0],{dataTransfer:transfer});
        expect(s.dragging.value).toEqual(['1','3']);
        s.overTask(s.data.value.tasks[1],{preventDefault:vi.fn(),stopPropagation:vi.fn(),clientX:20,currentTarget:{getBoundingClientRect:()=>({left:0,width:30})}});
        expect(s.insertion.value).toEqual({engineer_id:'a',anchor_id:'2',side:'after'});
        await s.drop({preventDefault:vi.fn(),stopPropagation:vi.fn()});
        const sent=JSON.parse(h.fetch.mock.calls[1][1].body);
        expect(sent).toMatchObject({kind:'move',task_ids:['1','3'],anchor_id:'2',side:'after'});
        s.clickTask(s.data.value.tasks[0],{}); expect(h.emit).not.toHaveBeenCalled();
    });
    it('retries a lost response with exactly the same operation ID and body', async () => {
        const h=harness([data(),new Error('connection lost'),{...data(),revision:1}]); await h.state.load();
        await h.state.operate('move',{task_ids:['1'],engineer_id:'b'});
        expect(h.state.pending.value).not.toBeNull(); expect(h.state.locked.value).toBe(true);
        const body=h.fetch.mock.calls[1][1].body;
        await h.state.send();
        expect(h.fetch.mock.calls[2][1].body).toBe(body);
        expect(h.state.pending.value).toBeNull(); expect(h.state.data.value.revision).toBe(1);
    });
    it('refreshes a conflicting revision without replaying the rejected move', async () => {
        const h=harness([data(),{detail:{message:'Очередь изменена'}},{...data(),revision:2}]); await h.state.load();
        await h.state.operate('move',{task_ids:['1'],engineer_id:'b'});
        expect(h.state.data.value.revision).toBe(2); expect(h.state.error.value).toBe('Очередь изменена');
        expect(h.state.pending.value).toBeNull(); expect(h.fetch).toHaveBeenCalledTimes(3);
    });
    it('does not lose text when closing preparation, clears it only on a confirmed save', async () => {
        const h=harness([data(),{...data(),revision:1}]); await h.state.load();
        const close=vi.fn(); h.state.prepDialog.value={close}; h.state.drafts.value={a:'Unfinished'};
        h.state.closePreparation(); expect(h.state.drafts.value.a).toBe('Unfinished');
        await h.state.operate('add_preparation',{items:[{engineer_id:'a',title:'Unfinished'}]});
        expect(h.state.drafts.value).toEqual({}); expect(close).toHaveBeenCalledTimes(2);
    });
    it('offers unique portal objects and submits a Shift-compatible multi-selection as one assignment', async () => {
        const initial = catalogData(), saved = {...initial, revision:1};
        const h = harness([initial, saved]); await h.state.load();
        expect([...h.state.objectOptions.value]).toEqual(['213. King & Sons', '272. Балчуг Эстейт']);
        h.state.search.value = 'king';
        expect([...h.state.suggestedObjects.value]).toEqual(['213. King & Sons']);
        h.state.selectObject('272. Балчуг Эстейт');
        expect(h.state.search.value).toBe('272. Балчуг Эстейт');
        expect(h.state.objectMenuOpen.value).toBe(false);
        h.state.search.value = '';
        expect(h.state.available.value.map(item => item.ready)).toEqual([true, true, false]);
        h.state.chooseProject('1', {});
        h.state.chooseProject('2', {shiftKey:true});
        expect([...h.state.chosenProjectIndexes.value]).toEqual(['1', '0', '2']);
        h.state.chosenProject.value = ['0', '2'];
        h.state.targetEngineer.value = 'a';
        await h.state.saveCatalog();
        const sent = JSON.parse(h.fetch.mock.calls[1][1].body);
        expect(sent.kind).toBe('assign');
        expect(sent.items.map(item => [item.project_id, item.engineer_id])).toEqual([['AR/P2','a'],['KJ/P3','a']]);
    });
    it('adds the double-clicked project immediately and removes it from the returned catalog', async () => {
        const initial = catalogData();
        const saved = {...initial, revision:1, available_projects:initial.available_projects.filter(item => item.project_id !== 'AR/P1')};
        const h = harness([initial, saved]); await h.state.load();
        const close = vi.fn();
        h.state.catalogOpen.value = true;
        h.state.catalogDialog.value = {open:true, close};
        h.state.targetEngineer.value = 'b';
        h.state.chosenProject.value = ['0', '2'];
        await h.state.quickAddProject('1');
        const sent = JSON.parse(h.fetch.mock.calls[1][1].body);
        expect(sent).toMatchObject({kind:'assign', items:[{project_id:'AR/P1',engineer_id:'b'}]});
        expect(h.state.data.value.available_projects.map(item => item.project_id)).toEqual(['AR/P2','KJ/P3']);
        expect(h.state.catalogOpen.value).toBe(true);
        expect(h.state.chosenProjectIndexes.value).toEqual([]);
        expect(close).not.toHaveBeenCalled();
    });
});
