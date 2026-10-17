import {it, expect} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

it('loads highlights and opens the correct result when A/B change, without leaking another pair', async () => {
    const watchers = [];
    const detailed = id => ({run_id: id, scope: 'all', state: 'done',
        a: {version_id: id === 'old' ? 'v1' : 'v2'}, b: {version_id: id === 'old' ? 'v2' : 'v3'},
        items: [{row_id: 'row', spid: id + '-sheet', kind: 'compare', status: 'done', result_status: 'different'}]});
    let component;
    const sandbox = {Vue: {
        ref: value => ({value}), reactive: x => x, computed: f => ({get value() {return f();}}),
        watch: (source, cb) => watchers.push(cb), onMounted: () => {}, onBeforeUnmount: () => {},
    }, fetch: async url => ({ok: true, json: async () => detailed(url.split('/').pop())})};
    sandbox.globalThis = sandbox;
    vm.runInNewContext(readFileSync(new URL('../static/js/version-comparison.js', import.meta.url), 'utf8'), sandbox);
    sandbox.VersionComparison.register({component: (name, def) => {if(name === 'version-comparison-page') component = def;}});
    const page = component.setup(), p = {project_id: 'P'}, row = {row_id: 'row'};
    page.pairs.P = {a: 'v2', b: 'v3'};
    page.activeRun.P = detailed('new');
    page.runs.P = ['new','old'].map(id => {const r = detailed(id); delete r.items; return r;});
    expect(page.cellClass(p, row, {version_id: 'v3'})['is-diff']).toBe(true);
    page.setSide(p, 'a', 'v1'); page.setSide(p, 'b', 'v2');
    expect(page.cellClass(p, row, {version_id: 'v2'})['is-diff']).toBe(false);
    watchers.forEach(cb => cb());
    await new Promise(resolve => setImmediate(resolve));
    expect(page.cellClass(p, row, {version_id: 'v2'})['is-diff']).toBe(true);
    expect(page.cellClass(p, row, {version_id: 'v3'})['is-diff']).toBe(false);
    // Щелчок по листу ставит пару «предыдущая → версия листа» и открывает её результат.
    page.matrices.P = {state: 'ready', versions: ['v1', 'v2', 'v3'].map(version_id => ({version_id}))};
    await page.clickCell(p, row, {version_id:'v2'}, {page:1}, {});
    expect(page.viewer.run.run_id).toBe('old');
    expect(page.viewer.spid).toBe('old-sheet');
    page.pairs.P = {a:'v1', b:'v3'};
    expect(page.pairHasRun(p)).toBe(false);
    expect(page.cellClass(p, row, {version_id:'v3'})['is-diff']).toBe(false);
});
