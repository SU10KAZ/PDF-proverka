const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.resolve(__dirname, '../..');

function harness() {
    const requests = [], confirmations = [];
    const ctx = vm.createContext({console, setTimeout, clearTimeout, confirm: text => { confirmations.push(text); return true; }});
    ctx.window = ctx;
    vm.runInContext(fs.readFileSync(path.join(root, 'frontend/static/js/vue.global.prod.js'), 'utf8'), ctx);
    let component, stop;
    ctx.Vue.onMounted = () => {};
    ctx.Vue.onBeforeUnmount = cb => { stop = cb; };
    ctx.VersionComparison = {api: async (url, options) => {
        requests.push({url, options});
        return {state: 'running', phase: 'specifications', scope: ['project'], settings: {model: 'gpt-6-luna'},
            pipeline: {state: 'running', progress: {done: 3, total: 5},
                current: {code: 'ЭМ1', label: 'Полные спецификации'}}};
    }};
    vm.runInContext(fs.readFileSync(path.join(root, 'frontend/static/js/version-comparison-luna.js'), 'utf8'), ctx);
    ctx.VersionComparisonLuna.register({component: (_, c) => { component = c; }});
    const state = component.setup({objectId: 'obj'}, {emit: () => {}});
    const render = ctx.Vue.compile(component.template, {decodeEntities: value => value});
    const texts = v => typeof v === 'string' ? [v] : Array.isArray(v) ? v.flatMap(texts) : v && v.children ? texts(v.children) : [];
    return {requests, confirmations, state, stop: () => stop(), text: () => texts(render(ctx.Vue.proxyRefs({objectId: 'obj', ...state}), [])).join(' ')};
}

test('one confirmation submits one pipeline request with disclosure of both routes', async () => {
    const h = harness();
    try {
        await h.state.start();
        assert.equal(h.requests.length, 1);
        assert.equal(h.requests[0].url, '/objects/obj/ai-sheet-match');
        assert.equal(h.requests[0].options.method, 'POST');
        assert.equal(h.requests[0].options.body.consent, true);
        assert.ok(h.confirmations[0].includes('полные спецификации'));
        assert.ok(h.confirmations[0].includes('общий MD'));
    } finally { h.stop(); }
});

test('the existing Luna bar displays the current specification stage and overall progress', async () => {
    const h = harness();
    try {
        await h.state.start();
        const text = h.text();
        for (const label of ['Полные спецификации', 'этапов 3 из 5', 'ЭМ1', 'Процесс: идёт']) {
            assert.ok(text.includes(label), label);
        }
    } finally { h.stop(); }
});
