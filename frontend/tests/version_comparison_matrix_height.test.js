import {it, expect} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

it('gives the matrix viewport height below the sticky header regardless of the summary above', () => {
    let page;
    const body = {};
    const parent = {parentElement: body, clientHeight: 900, querySelector: () => ({getBoundingClientRect: () => ({height: 50})})};
    const el = {parentElement: parent, style: {}};
    const sandbox = {document: {body}, getComputedStyle: () => ({overflowY: 'auto'}),
        requestAnimationFrame: f => f(), window: {innerHeight: 900, addEventListener() {}, removeEventListener() {}}};
    sandbox.globalThis = sandbox;
    vm.runInNewContext(readFileSync(new URL('../static/js/version-comparison.js', import.meta.url), 'utf8'), sandbox);
    sandbox.window.VersionComparison.register({component: (name, def) => {page = def;}});
    page.directives.fitBottom.mounted(el);
    expect(el.style.maxHeight).toBe('842px');
    parent.clientHeight = 650;
    page.directives.fitBottom.updated(el);
    expect(el.style.maxHeight).toBe('592px');
    page.directives.fitBottom.unmounted(el);
});
