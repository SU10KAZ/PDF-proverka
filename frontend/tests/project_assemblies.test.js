import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../static/js/project-assemblies.js', import.meta.url), 'utf8');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const appSource = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

function load(enabled, fetchImpl) {
    const ref = value => ({value});
    const computed = fn => ({get value() { return fn(); }});
    const watch = (_source, handler, options) => { if (options?.immediate) handler(); };
    let definition;
    const sandbox = {
        Vue: {ref, computed, watch}, fetch: fetchImpl,
        document: {getElementById: () => ({textContent: JSON.stringify({enabled})})},
        crypto: {randomUUID: () => 'request-id'},
    };
    sandbox.globalThis = sandbox;
    vm.runInNewContext(source, sandbox);
    sandbox.ProjectAssemblies.register({component: (_name, value) => { definition = value; }});
    return {definition};
}

describe('Project assemblies feature gate and wiring', () => {
    it('does not request anything while the server capability is off', async () => {
        const calls = [];
        const {definition} = load(false, async url => { calls.push(url); return {}; });
        definition.setup({objectId: 'obj'}, {emit: () => {}});
        await Promise.resolve();
        expect(calls).toEqual([]);
    });

    it('loads sources and assemblies only when enabled', async () => {
        const calls = [];
        const {definition} = load(true, async url => {
            calls.push(url);
            return {ok: true, json: async () => ({items: []})};
        });
        definition.setup({objectId: 'obj'}, {emit: () => {}});
        await new Promise(resolve => setTimeout(resolve, 0));
        expect(calls).toEqual([
            '/api/project-assemblies/sources?object_id=obj',
            '/api/project-assemblies?object_id=obj',
        ]);
    });

    it('is exposed as a gated tab in the comparison page', () => {
        expect(html).toContain("scTab==='assemblies'");
        expect(html).toContain('<project-assemblies v-if="scTab === \'assemblies\' && projectAssembliesEnabled"');
        expect(html).toContain('id="project-assemblies-caps"');
        expect(appSource).toContain('window.ProjectAssemblies.register(app);');
    });
});
