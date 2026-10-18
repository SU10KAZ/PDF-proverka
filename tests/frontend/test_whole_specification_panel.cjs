// node --test tests/frontend/test_whole_specification_panel.cjs
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.resolve(__dirname, '../..');

function harness() {
    const ctx = vm.createContext({console, setTimeout, clearTimeout});
    vm.runInContext(fs.readFileSync(path.join(root, 'frontend/static/js/vue.global.prod.js'), 'utf8'), ctx);
    const requests = [], states = [];
    let mounted, unmounted, component;
    ctx.Vue.onMounted = fn => { mounted = fn; };
    ctx.Vue.onBeforeUnmount = fn => { unmounted = fn; };
    ctx.VersionComparison = {API: '/api/version-comparison', api: url => new Promise(resolve => requests.push({url, resolve}))};
    vm.runInContext(fs.readFileSync(path.join(root, 'frontend/static/js/version-comparison-sheet-changes.js'), 'utf8'), ctx);
    ctx.VersionComparisonSheetChanges.register({component: (_, c) => { component = c; }});
    const props = ctx.Vue.reactive({objectId: 'obj', run: {run_id: 'run1'}, spid: 'spec1'});
    const panel = component.setup(props, {emit: (...args) => states.push(args)});
    // The template has no HTML entities; this keeps compilation independent of a DOM.
    const render = ctx.Vue.compile(component.template, {decodeEntities: value => value});
    return {ctx, props, panel, requests, states, mounted, unmounted,
        text: () => texts(render(ctx.Vue.proxyRefs(panel), [])).join(' ')};
}

function texts(vnode) {
    if (typeof vnode === 'string') return [vnode];
    if (Array.isArray(vnode)) return vnode.flatMap(texts);
    return vnode && vnode.children ? texts(vnode.children) : [];
}

function result(changes = [], state = 'done') {
    return {source: 'whole_specification', title: 'Спецификация', a: {label: 'V1', page: 2},
        b: {label: 'V2', page: 5}, changes, state, state_label: 'Изменения спецификации', unresolved: []};
}

const change = {id: 'e1', source: 'whole_specification', object: 'Кабель', summary: 'Изменено',
    place: 'Старая: л.1 (стр. PDF 2); новая: л.2 (стр. PDF 6).',
    fields: [{name: 'Количество', before: '10 м', after: '12 шт.'},
        {name: 'Марка', before: 'AB-1', after: 'AB-2'}, {name: 'Поставщик', before: 'Первый', after: 'Второй'}]};

test('renders every changed field and both source locations, without legacy drawing actions', async () => {
    const h = harness();
    const loading = h.mounted();
    h.requests[0].resolve(result([change]));
    await loading;
    const text = h.text();
    for (const expected of ['только изменения этого листа', 'Количество', '10 м', '12 шт.',
        'Марка', 'AB-1', 'AB-2', 'Поставщик', 'Первый', 'Второй', change.place, 'MD листа']) {
        assert.ok(text.includes(expected), expected);
    }
    assert.ok(!text.includes('Показать на чертеже'));
    assert.ok(!text.includes('Описать этот лист'));
    assert.deepEqual(h.states[0], ['state', 'done', 'whole_specification']);
    h.unmounted();
});

test('switching version pair with the same sheet id discards late old replies', async () => {
    const h = harness();
    const first = h.mounted();
    h.props.run = {run_id: 'run2'};
    await h.ctx.Vue.nextTick();
    assert.equal(h.requests.length, 2);
    assert.ok(h.requests[1].url.includes('/runs/run2/sheets/spec1/changes'));
    const quiet = result([], 'no_construction_changes');
    quiet.message = 'В содержании спецификации на этом листе изменений не выявлено.';
    h.requests[1].resolve(quiet);
    await Promise.resolve();
    h.requests[0].resolve(result([change]));
    await first;
    assert.equal(h.panel.data.value.state, 'no_construction_changes');
    assert.ok(h.text().includes(quiet.message));
    assert.ok(!h.text().includes('Кабель'));
    h.unmounted();
});

test('unavailable full comparison is explicit and cannot trigger the sheet AI', async () => {
    const h = harness();
    const loading = h.mounted();
    const unavailable = result([], 'specification_unavailable');
    unavailable.message = 'Отсутствие изменений на этом листе пока не установлено.';
    h.requests[0].resolve(unavailable);
    await loading;
    assert.ok(h.text().includes(unavailable.message));
    assert.ok(!h.text().includes('Описать этот лист'));
    assert.ok(!h.text().includes('Строительных изменений не выявлено'));
    h.unmounted();
});

test('a reply received after closing the viewer cannot change its state', async () => {
    const h = harness();
    const loading = h.mounted();
    h.unmounted();
    h.requests[0].resolve(result([change]));
    await loading;
    assert.equal(h.panel.data.value, null);
    assert.equal(h.states.length, 0);
});
