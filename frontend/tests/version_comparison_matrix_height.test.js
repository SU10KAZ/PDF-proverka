import {it, expect} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

it('gives the matrix viewport height below the sticky header regardless of the summary above', () => {
    let page;
    const body = {};
    const parent = {parentElement: body, clientHeight: 900, querySelector: () => ({getBoundingClientRect: () => ({height: 50})})};
    const bodyEl = {querySelector: () => ({getBoundingClientRect: () => ({height: 42})})};
    const el = {parentElement: parent, style: {}, closest: () => bodyEl};
    const sandbox = {document: {body}, getComputedStyle: () => ({overflowY: 'auto', display: 'block', rowGap: '10px', paddingBottom: '12px', borderBottomWidth: '0px', marginBottom: '0px'}),
        requestAnimationFrame: f => f(), window: {innerHeight: 900, addEventListener() {}, removeEventListener() {}}};
    sandbox.globalThis = sandbox;
    vm.runInNewContext(readFileSync(new URL('../static/js/version-comparison.js', import.meta.url), 'utf8'), sandbox);
    sandbox.window.VersionComparison.register({component: (name, def) => {page = def;}});
    page.directives.fitBottom.mounted(el);
    expect(el.style.maxHeight).toBe('778px');
    parent.clientHeight = 650;
    page.directives.fitBottom.updated(el);
    expect(el.style.maxHeight).toBe('528px');
    page.directives.fitBottom.unmounted(el);
});
