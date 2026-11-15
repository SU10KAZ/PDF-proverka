const {test} = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const source = fs.readFileSync('frontend/static/js/app.js', 'utf8');
function extract(from, to) {
    const start = source.indexOf(from), end = source.indexOf(to, start);
    assert(start >= 0 && end > start);
    return source.slice(start, end);
}
const plain = value => JSON.parse(JSON.stringify(value));
const doc = pdf_path => ({pdf_path});

test('new uploads on both sides append separately and preserve reserved empty cells', () => {
    const h = vm.createContext({});
    vm.runInContext(extract('        function scReconcileDocumentOrder(', '        function scCompactDocumentOrder('), h);
    const result = h.scReconcileDocumentRows(['A', 'B', null], ['C', null, 'D'],
        ['A', 'B', 'E'].map(doc), ['C', 'D', 'F'].map(doc));
    assert.deepEqual(plain(result), {left: ['A', 'B', null, 'E', null], right: ['C', null, 'D', null, 'F']});
});

test('both absent documents never restore a phantom confirmation', () => {
    const h = vm.createContext({scConfirmedDocumentPairs: {}, scDocumentOrder: {left: ['A'], right: ['B']}});
    vm.runInContext(extract('        function scPairPathsKey(', '        function scReconcileDocumentOrder('), h);
    h.scRestoreConfirmedDocumentPairs([{left_pdf: 'deleted-left', right_pdf: 'deleted-right'}, {left_pdf: 'A', right_pdf: 'B'}]);
    assert.deepEqual(plain(h.scConfirmedDocumentPairs), {'A::B': {leftPdf: 'A', rightPdf: 'B'}});
});

function savesHarness() {
    const requests = [], pending = [];
    const ref = value => ({value});
    const h = vm.createContext({window: {portalCan: {operate: () => true}},
        scSession: ref({id: 'session', document_pairing: {revision: 4}}),
        scPairingMatching: ref(false), scPairingSaving: ref(false), scPairingDirty: ref(true),
        scPairingSaveError: ref(''), scPairingSaveMessage: ref(''), scPairingSaveTimer: 0,
        order: ['A'], scPersistDocumentOrder() {}, clearTimeout,
        fetch: async (url, options) => { requests.push(JSON.parse(options.body)); return new Promise(resolve => pending.push(resolve)); },
    });
    vm.runInContext('function scDocumentPairingPayload() { return {left_order: [...order], right_order: ["B"], confirmed_pairs: [], expected_revision: scSession.value.document_pairing.revision}; }', h);
    vm.runInContext(extract('        let scPairingSavePending =', '        const scBoardRemoving ='), h);
    return {h, requests, pending};
}
const response = (revision, ok = true) => ({ok, status: ok ? 200 : 409, json: async () => ok ? {revision} : {detail: 'conflict'}});

test('refresh flush waits for a pending save then persists later edits with the next revision', async () => {
    const {h, requests, pending} = savesHarness();
    const saving = h.scSaveDocumentPairing();
    h.order = ['changed-while-saving'];
    const flushing = h.scFlushDocumentPairing();
    assert.equal(requests.length, 1);
    pending.shift()(response(5));
    await saving;
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(requests.length, 2);
    assert.equal(requests[1].expected_revision, 5);
    assert.deepEqual(requests[1].left_order, ['changed-while-saving']);
    pending.shift()(response(6));
    assert.equal(await flushing, true);
    assert.equal(h.scPairingDirty.value, false);
});

test('a stale save fails visibly and prevents refresh from discarding unsaved changes', async () => {
    const {h, requests, pending} = savesHarness();
    const flushing = h.scFlushDocumentPairing();
    pending.shift()(response(4, false));
    assert.equal(await flushing, false);
    assert.equal(h.scPairingDirty.value, true);
    assert.equal(h.scPairingSaveError.value, 'conflict');
    assert.equal(requests.length, 1);
});

for (const side of ['left', 'right', 'both', 'group']) {
    test(`delete ${side}: await save, send selected identifiers, install authoritative board`, async () => {
        const calls = [], ref = value => ({value});
        const updated = {id: 'session', document_pairing: {revision: 8, left_order: [], right_order: []}};
        const h = vm.createContext({ref, window: {portalCan: {operate: () => true}, confirm: () => true},
            pcReadOnlySources: ref(false), scPairRowBusy: () => false, scGroupPending: () => false,
            scSession: ref({id: 'session', document_pairing: {revision: 6}}),
            scDocumentGroupsError: ref(''), scPairingSaveError: ref(''), scPairingSaveMessage: ref(''),
            scFlushDocumentPairing: async () => { calls.push('flush'); h.scSession.value.document_pairing.revision = 7; return true; },
            fetch: async (url, options) => { calls.push({url, method: options.method, payload: JSON.parse(options.body)}); return {ok: true, json: async () => updated}; },
            scInitializeDocumentOrder: saved => calls.push(['initialize', saved]),
            scPersistDocumentOrder: dirty => calls.push(['persist', dirty]),
        });
        vm.runInContext(extract('        const scBoardRemoving =', '        async function scAutoMatchDocumentProjects('), h);
        const row = {left: doc('A'), right: doc('B'), ...(side === 'group' ? {group: {group_id: 'group1'}} : {})};
        await h.scDeleteBoardRow(row, side === 'group' ? 'both' : side);
        const expected = {expected_revision: 7};
        if (side === 'group') expected.group_id = 'group1';
        else { if (side !== 'right') expected.left_pdf = 'A'; if (side !== 'left') expected.right_pdf = 'B'; }
        assert.deepEqual(calls.slice(0, 2), ['flush', {url: '/api/stage-comparison/sessions/session/board-documents', method: 'DELETE', payload: expected}]);
        assert.equal(h.scSession.value, updated);
        assert.deepEqual(calls.slice(2), [['initialize', true], ['persist', false]]);
        assert.equal(vm.runInContext('scBoardRemoving.value', h), false);
        assert.equal(h.scDocumentGroupsError.value, '');
    });
}
