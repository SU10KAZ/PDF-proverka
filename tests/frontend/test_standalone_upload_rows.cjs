const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.resolve(__dirname, '../..');
const source = fs.readFileSync(path.join(root, 'frontend/static/js/app.js'), 'utf8');
function functions(from, to) { return source.slice(source.indexOf(from), source.indexOf(to, source.indexOf(from))); }
function harness() {
    const ctx = vm.createContext({
        window: {portalCan: {operate: () => true}},
        currentObjectId: {value: 'obj'}, pcReadOnlySources: {value: false},
        scStandaloneUploadRows: {value: []},
        scDocumentOrder: {left: ['L1', 'L2', null], right: [null, 'R1', 'R2']},
        scBoardDocumentsRight: {value: [{document_code: 'A', pdf_path: 'R1'}, {document_code: 'B', pdf_path: 'R2'}]},
        scLeftDocumentByCode: code => code === 'IOS' ? {document_code: 'IOS', pdf_path: 'L1'} : null,
        localStorage: {setItem() {}},
        scPersistDocumentOrder() {},
        scRemoveConfirmedPairsForPaths() {},
        scOpenStageFolderDialog() {},
        scStageFolderDialogOpen: {value: false}, scStageFolderDialogStage: {value: ''}, scStageUploadTarget: {value: null},
    });
    ctx.scPersistDocumentOrder = () => { ctx.saved = true; };
    ctx.scRemoveConfirmedPairsForPaths = paths => { ctx.unconfirmed = paths; };
    ctx.scOpenStageFolderDialog = () => { ctx.scStageFolderDialogOpen.value = true; };
    vm.runInContext(functions('        function scPackDocumentRows(', '        function scCompactDocumentOrder('), ctx);
    vm.runInContext(functions('        function scStandaloneUploadKey(', '        // Перетащили правый документ на «+» строки'), ctx);
    return ctx;
}
const plain = value => JSON.parse(JSON.stringify(value));
test('uploads move under their anchor with empty left cells and preserve other left documents', () => {
    const h = harness();
    h.scSetStandaloneUploadRow({left: {document_code: 'IOS'}}, true);
    assert.equal(h.scPlaceStandaloneRightDocuments('IOS', ['A', 'B']), '');
    assert.deepEqual(plain(h.scDocumentOrder), {left: ['L1', null, null, 'L2'], right: [null, 'R1', 'R2', null]});
    assert.equal(h.saved, true);
    assert.deepEqual(plain(h.unconfirmed), ['R1', 'R2']);
    assert.deepEqual(plain(h.scStandaloneUploadRows.value), []);
});
test('a standalone upload target does not reference the existing left group', () => {
    const h = harness();
    h.scOpenStandaloneRowUpload({left: {document_code: 'IOS', filename: 'IOS.pdf'}});
    assert.deepEqual(plain(h.scStageUploadTarget.value), {afterLeftCode: 'IOS', afterLeftName: 'IOS.pdf'});
    assert.equal(h.scStageFolderDialogStage.value, 'stage_2');
});
test('drafts are scoped to the object and viewers cannot add them', () => {
    const h = harness();
    const row = {left: {document_code: 'IOS'}};
    h.scSetStandaloneUploadRow(row, true);
    assert.equal(h.scHasStandaloneUploadRow(row), true);
    h.currentObjectId.value = 'other';
    assert.equal(h.scHasStandaloneUploadRow(row), false);
    h.window.portalCan.operate = () => false;
    h.scSetStandaloneUploadRow(row, true);
    assert.equal(h.scHasStandaloneUploadRow(row), false);
});
test('hidden group members cannot be moved into a standalone row', () => {
    const h = harness();
    const before = plain(h.scDocumentOrder);
    assert.ok(h.scPlaceStandaloneRightDocuments('IOS', ['HIDDEN']));
    assert.deepEqual(plain(h.scDocumentOrder), before);
});
test('the modified board template compiles', () => {
    const ctx = vm.createContext({console});
    vm.runInContext(fs.readFileSync(path.join(root, 'frontend/static/js/vue.global.prod.js'), 'utf8'), ctx);
    const html = fs.readFileSync(path.join(root, 'frontend/index.html'), 'utf8');
    const start = html.indexOf('<template v-for="row in scPairRows"');
    const end = html.indexOf('<div v-if="scTab===\'links\'"', start);
    const board = html.slice(start, end).replace(/<\/div>\s*<\/section>\s*<\/div>\s*$/, '');
    const errors = [];
    ctx.Vue.compile(board, {decodeEntities: value => value, onError: error => errors.push(error.message)});
    assert.deepEqual(errors, []);
});
