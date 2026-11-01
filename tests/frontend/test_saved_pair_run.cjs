const {test} = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../../frontend/static/js/app.js'), 'utf8');
function extract(from, to) { const start = source.indexOf(from); return source.slice(start, source.indexOf(to, start)); }
test('opening saved work installs its original session and never creates a fresh pair', async () => {
    const calls = [];
    const pair = {id: 'original'};
    const h = vm.createContext({scPairRowBusy: () => false, scSelectedObject: {value: {id: 'object'}},
        scPairs: {value: [pair]}, scSessionError: {value: ''},
        pcEnsureResultSession: async (...args) => calls.push(['session', ...args]),
        scOpenPair: async value => { calls.push(['open', value.id]); return {pair: value}; },
        scBeginPairOpen: () => { throw Error('must not create a new pair'); }});
    vm.runInContext(extract('        async function scOpenPairRow(row)', '        async function scOpenPairRowResult('), h);
    const opened = await h.scOpenPairRow({left: {}, right: {}, comparisonRun: {session_id: 'previous', pair_id: 'original'}});
    assert.equal(opened.pair.id, 'original');
    assert.deepEqual(calls, [['session', 'previous', 'object'], ['open', 'original']]);
});
test('catalog and board show saved mapping and mining progress', () => {
    const h = vm.createContext({scGroupStatus: () => null});
    vm.runInContext(extract('        function scPairRowStatus(row)', '        function scPairRowBusy('), h);
    vm.runInContext(extract('        function scCatalogPairStatus(row)', '        function scStartCatalogPair('), h);
    const row = {comparisonRun: {state: {status: 'RUNNING', saved_mapping_portions: 110,
        current_stage: 'MINING', processed: 7, total: 155}}};
    assert.equal(h.scCatalogPairStatus(row), '110 порций сохранено · Поиск изменений 7/155');
});
test('empty block view reports persisted portion count without mistaking it for block count', () => {
    const script = fs.readFileSync(path.join(__dirname, '../../frontend/static/js/stage-block-mapping.js'), 'utf8');
    const start = script.indexOf('                const progressMessage = computed(');
    const end = script.indexOf('                const viewport', start);
    const props = {runState: {status: 'RUNNING', provenance: {mapper_plan: {complete: true,
        accepted_portions: Array.from({length: 110}, (_, i) => i + 1)}}, message: 'Поиск изменений 7/155.'}};
    const h = vm.createContext({props, computed: fn => ({get value() { return fn(); }})});
    vm.runInContext(script.slice(start, end), h);
    const message = vm.runInContext('progressMessage.value', h);
    assert.match(message, /Сопоставление сохранено: 110 порций/);
    assert.match(message, /Поиск изменений 7\/155/);
    props.runState = null;
    assert.equal(vm.runInContext('progressMessage.value', h), '');
});
