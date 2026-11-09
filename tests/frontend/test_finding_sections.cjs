const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('frontend/static/js/app.js', 'utf8');
function sections(items) {
    const ctx = vm.createContext({
        sortedFindings: {value: items},
        computed: fn => ({get value() { return fn(); }}),
    });
    vm.runInContext(source.slice(source.indexOf('        const findingsHaveQuestions ='),
        source.indexOf('        const findingsTotalPages =')) +
        '\nglobalThis.rows = paginatedFindings.value;', ctx);
    return ctx;
}
test('questions form a separate section without changing IDs or within-section order', () => {
    const items = [
        {id:'Q2', report_section:'questions'}, {id:'T2', report_section:'technical_findings'},
        {id:'Q1', report_section:'questions'}, {id:'L1'},
    ];
    const h = sections(items);
    assert.deepEqual(Array.from(h.rows, f => f.id), ['T2','L1','Q2','Q1']);
    assert.deepEqual(h.rows.map((_, i) => h.findingSectionHeading(i)).join('|'),
        'Замечания||Вопросы и уточнения|');
    assert.equal(h.rows[2], items[0]);
    assert.deepEqual(items.map(f => f.id), ['Q2','T2','Q1','L1']);
});
test('filtered question-only and legacy-only views do not invent empty sections', () => {
    const q = sections([{id:'Q', report_section:'questions'}]);
    assert.equal(q.findingSectionHeading(0), 'Вопросы и уточнения');
    const legacy = sections([{id:'L'}]);
    assert.equal(legacy.findingSectionHeading(0), '');
    assert.equal(sections([]).rows.length, 0);
});
