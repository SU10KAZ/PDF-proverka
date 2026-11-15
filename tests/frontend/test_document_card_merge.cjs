const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('frontend/static/js/app.js', 'utf8');
function extract(from, to) {
    const start = source.indexOf(from);
    assert(start >= 0, from);
    const end = source.indexOf(to, start);
    assert(end > start, to);
    return source.slice(start, end);
}
const document = code => ({document_code: code, pdf_path: '/' + code, filename: code + '.pdf'});
const row = (index, left, right) => ({index, left: left && document(left), right: right && document(right), leftMembers: [], members: []});
const plain = value => JSON.parse(JSON.stringify(value));
function harness() {
    const calls = [];
    const rows = [row(0, 'L1', 'R1'), row(1, 'L2', 'R2')];
    const h = vm.createContext({window: {portalCan: {operate: () => true}},
        projectAssembliesEnabled: true, pcReadOnlySources: {value: false},
        scDocumentGroupsBusy: {value: false}, scDraggingDocument: {value: null},
        scDocumentDragOver: {left: null, right: null}, scDocumentMergeOver: {left: null, right: null},
        SC_OTHER_SIDE: {left: 'right', right: 'left'},
        scPairRows: {value: rows}, scDocumentOrder: {left: ['/L1', '/L2'], right: ['/R1', '/R2']},
        scPairRowBusy: row => !!row.busy, scPairRowDragLocked: row => !!(row.busy || row.secondary),
        scGroupPending: group => !!group.pending,
        scChangeDocumentGroups: async (url, options) => { calls.push({url, ...options, body: JSON.parse(options.body)}); return ''; },
        scGroupMemberDrag: {value: null}, scFinishGroupMemberDrag() {}, scMoveGroupMember() {},
    });
    vm.runInContext(extract('        function scRowSideCodes(', '        function scGroupSideStatus('), h);
    vm.runInContext(extract('        async function scAssignDocuments(', '        const scGroupMemberDrag'), h);
    vm.runInContext(extract('        function scDocumentMergeSource(', '        function scDragDocumentOver('), h);
    vm.runInContext(extract('        function scFinishDocumentDrag(', '        function scIsDraggingDocument('), h);
    vm.runInContext(extract('        function scDropGroupMember(', '        function scMoveGroupMember('), h);
    return {h, calls, rows};
}
const event = () => ({dataTransfer: {}, prevented: false, stopped: false,
    preventDefault() { this.prevented = true; }, stopPropagation() { this.stopped = true; }});

for (const side of ['left', 'right']) {
    test(`${side}: a card drop adds both documents and keeps the opposite side in place`, async () => {
        const {h, calls, rows} = harness();
        const before = plain(h.scDocumentOrder);
        h.scDraggingDocument.value = {side, index: 0};
        const over = event();
        h.scDragDocumentOnto(over, rows[1], side);
        assert.equal(h.scDocumentMergeOver[side], 1);
        assert.equal(over.dataTransfer.dropEffect, 'move');
        const drop = event();
        await h.scDropDocumentOnto(rows[1], side, drop);
        assert(drop.prevented && drop.stopped);
        assert.equal(calls.length, 1);
        assert.equal(calls[0].method, 'POST');
        assert.deepEqual(calls[0].body, {side, [side === 'left' ? 'right_document_code' : 'left_document_code']: side === 'left' ? 'R2' : 'L2',
            member_codes: side === 'left' ? ['L2', 'L1'] : ['R2', 'R1'], group_id: null});
        assert.deepEqual(plain(h.scDocumentOrder), before, 'No optimistic swap or loss of the source counterpart');
        assert.equal(h.scDraggingDocument.value, null);
        assert.equal(h.scDocumentMergeOver[side], null);
        await h.scDropDocumentOnto(rows[1], side, event());
        assert.equal(calls.length, 1, 'Bubbling/repeated drop cannot post twice');
    });

    test(`${side}: dropping on a group member appends to that side of the existing group`, async () => {
        const {h, calls, rows} = harness();
        const other = h.SC_OTHER_SIDE[side];
        rows[1].group = {group_id: 'g', [side]: {members: ['A', 'B'].map(document_code => ({document_code}))},
            [other]: {members: [{document_code: 'opposite'}]}};
        h.scDraggingDocument.value = {side, index: 0};
        await h.scDropGroupMember(rows[1], {document_code: 'A'}, side);
        assert.deepEqual(calls[0].body, {side, [other + '_document_code']: 'opposite',
            member_codes: ['A', 'B', side === 'left' ? 'L1' : 'R1'], group_id: 'g'});
    });
}

for (const reason of ['viewer', 'read-only', 'busy', 'source-busy', 'building', 'source-group', 'opposite-side', 'self', 'no-counterpart', 'duplicate']) {
    test(`rejects ${reason} without requests or rearranging documents`, async () => {
        const {h, calls, rows} = harness();
        h.scDraggingDocument.value = {side: 'left', index: 0};
        let target = rows[1];
        if (reason === 'viewer') h.window.portalCan.operate = () => false;
        if (reason === 'read-only') h.pcReadOnlySources.value = true;
        if (reason === 'busy') h.scDocumentGroupsBusy.value = true;
        if (reason === 'source-busy') rows[0].busy = true;
        if (reason === 'building') target.group = {pending: true, left: {members: [{document_code: 'L2'}]}, right: {members: [{document_code: 'R2'}]}};
        if (reason === 'source-group') rows[0].group = {};
        if (reason === 'opposite-side') h.scDraggingDocument.value.side = 'right';
        if (reason === 'self') target = rows[0];
        if (reason === 'no-counterpart') target.right = null;
        if (reason === 'duplicate') target.left = rows[0].left;
        const before = plain(h.scDocumentOrder), over = event();
        h.scDragDocumentOnto(over, target, 'left');
        assert.equal(over.dataTransfer.dropEffect, 'none');
        assert.equal(h.scDocumentMergeOver.left, null);
        await h.scDropDocumentOnto(target, 'left', event());
        assert.equal(calls.length, 0);
        assert.deepEqual(plain(h.scDocumentOrder), before);
    });
}
test('failed persistence leaves the original layout and propagates the group error', async () => {
    const {h, rows} = harness(), before = plain(h.scDocumentOrder);
    h.scChangeDocumentGroups = async () => 'Не удалось сохранить группу';
    h.scDraggingDocument.value = {side: 'right', index: 0};
    assert.equal(await h.scDropDocumentOnto(rows[1], 'right', event()), 'Не удалось сохранить группу');
    assert.deepEqual(plain(h.scDocumentOrder), before);
});
test('whole-row dragging and disabled assemblies retain their existing drop handlers', () => {
    const {h, rows} = harness();
    for (const disabled of [false, true]) {
        h.projectAssembliesEnabled = !disabled;
        h.scDraggingDocument.value = disabled ? {side: 'left', index: 0} : null;
        const e = event();
        h.scDragDocumentOnto(e, rows[1], 'left');
        h.scDropDocumentOnto(rows[1], 'left', e);
        assert(!e.prevented && !e.stopped);
    }
});
