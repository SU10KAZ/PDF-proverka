const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('frontend/static/js/app.js', 'utf8');
function harness() {
    const ctx = vm.createContext({ref: value => ({value}), window: {portalCan: {operate: () => true}},
        pcReadOnlySources: {value: false}, scDocumentGroupsBusy: {value: false},
        scPairRowBusy: () => false, SC_GROUP_BUILDING: ['QUEUED', 'PREPARING', 'RUNNING'],
        scChangeDocumentGroups: async (url, options) => ({url, body: JSON.parse(options.body)}),
    });
    vm.runInContext(source.slice(source.indexOf('        const scGroupMemberDrag = ref('),
        source.indexOf('        function scRemoveGroupMember(')), ctx);
    return ctx;
}
const row = () => ({group: {group_id: 'g1', status: 'READY'}, members: ['A', 'B', 'C'].map(document_code => ({document_code}))});
const plain = value => JSON.parse(JSON.stringify(value));
test('arrows persist the full member order without mutating the displayed row', async () => {
    const h = harness(), r = row();
    assert.deepEqual(plain(await h.scMoveGroupMember(r, 'C', 0)), {url: '/g1/order', body: {member_codes: ['C', 'A', 'B']}});
    assert.deepEqual(r.members.map(m => m.document_code), ['A', 'B', 'C']);
    assert.equal(h.scMoveGroupMember(r, 'A', -1), undefined);
});
test('dragging only reorders within the same group', async () => {
    const h = harness(), r = row();
    const event = {dataTransfer: {setData() {}}, preventDefault() {}};
    h.scStartGroupMemberDrag(event, r, r.members[0]);
    assert.equal(h.scDropGroupMember({...r, group: {group_id: 'other'}}, r.members[2]), undefined);
    h.scStartGroupMemberDrag(event, r, r.members[0]);
    assert.deepEqual(plain(await h.scDropGroupMember(r, r.members[2])).body.member_codes, ['B', 'C', 'A']);
});
test('viewers, read-only sources and building groups cannot reorder', () => {
    const h = harness(), r = row();
    h.window.portalCan.operate = () => false;
    assert.equal(h.scMoveGroupMember(r, 'A', 1), undefined);
    h.window.portalCan.operate = () => true;
    h.pcReadOnlySources.value = true;
    assert.equal(h.scMoveGroupMember(r, 'A', 1), undefined);
    h.pcReadOnlySources.value = false;
    r.group.status = 'RUNNING';
    assert.equal(h.scMoveGroupMember(r, 'A', 1), undefined);
});
