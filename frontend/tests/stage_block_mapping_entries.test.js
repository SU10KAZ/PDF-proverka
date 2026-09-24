// Phase E: entries into the stage-2 semantic blocks — deep link (MASTER §7, UNIFIED_UX_DESIGN §10),
// catalog button and catalog focus (C13), and the Human Mapping link without an empty scope (П-18).
// node:vm with fake fetch and fake refs; no network, no DOM.
import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import vm from 'node:vm';

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8');
const coreSource = read('../static/js/human-mapping-core.js');
const source = read('../static/js/stage-block-mapping.js');
const catalogSource = read('../static/js/project-comparison-catalog.js');
const app = read('../static/js/app.js');
const html = read('../index.html');

const SID = 'e6fc8a2725eb4a67', PID = 'p290a06df79', OID = '4f3e5916';
const RUN = 'a631b49aaaac4db0af66a495c155c629', OTHER = '7f3c21aa0000000000000000000000ff';
const SNAP = 'pcv3snap_5157938d548fb1df6fbcf87c200c1347';
const G20 = 'blk_d2ebac1ee4424ad596bcbe459b211e88', T10 = 'blk_55bd5531';

// DEV5-shaped regions (pages of run a631b49a): A-R001 touches row 5↔4, A-R003 no two-sided row, A-R016 row 20↔10.
function region(id, olds, news, shared = {}) {
    const members = side => (side === 'OLD' ? olds : news).map(p => ({id: shared[side + p] || `${id}:${side}:${p}`, page: p, type: 'TEXT'}));
    const m = {OLD: members('OLD'), NEW: members('NEW')};
    return {id, title: id, domain: id + ' домен', scope: '', membership_state: {OLD: 'MAPPED', NEW: 'MAPPED'},
        mapping_state: 'MAPPED', member_cardinality: 'N:N', pages: {OLD: olds, NEW: news}, members: m,
        allowed: {OLD: m.OLD.map(b => b.id), NEW: m.NEW.map(b => b.id)}, invalid_ref_count: 0};
}
const INDEX = {schema: 'stage-block-mapping-region-index/1', regions: [
    region('A-R001', [1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 6, 11]),
    region('A-R003', [9, 15], [15, 16, 23]),
    region('A-R016', [20, 51, 52], [5, 10, 26], {OLD20: G20, NEW10: T10}),
]};
const ROWS = [[5, 4], [6, 12], [20, 10]].map(([l, r], i) => ({key: 'explicit-' + i, leftPages: [l], rightPages: [r],
    source: 'auto', confidence: 'high', explicitLinkIndex: i}))
    .concat([{key: 'left-only-21', leftPages: [21], rightPages: [], source: 'unmatched', confidence: 'unmatched', explicitLinkIndex: null}]);
const run = (id, extra = {}) => ({run_id: id, state: 'COMPLETED_FROZEN', frozen: true, is_current: id === RUN,
    created_at: '2026-09-21T10:00:00+00:00', completed_at: '2026-09-21T12:00:00+00:00', engine_version: '3.5.2',
    model: 'claude-opus-5', model_display: 'Claude Opus 5', projectchange_count: 64, region_count: 3, hm_available: true,
    hm_reason: null, pdf_match: true, source_stale: false, blocks_content_match: null,
    history: {reviews: 0, block_link_events: 0, poisoned: false, poison_codes: []}, writable: false,
    write_block_reason: 'WRITES_DISABLED', write_block_reasons: ['WRITES_DISABLED'], ...extra});
const STATUS = {schema: 'stage-block-mapping-status/1', session_id: SID, pair_id: PID, object_id: OID, object_error: null,
    current_run_id: RUN, latest_attempt: null, runs: [run(RUN), run(OTHER)],
    snapshots: [{result_id: SNAP, hm_available: true, hm_url: '/human-mapping/?object=x', writable: false,
        write_block_reason: 'SNAPSHOT_READ_ONLY'}],
    source_blocks: {OLD: {pdf_sha256: 'a'.repeat(64)}, NEW: {pdf_sha256: 'b'.repeat(64)}}, capabilities: {block_mapping_writes: false}};
const hm = extra => ({object_id: OID, pair_key: PID, comparison_id: PID, run_id: RUN, result_id: null, ...extra});
const EDITS = [hm({event_id: 'e1', event_type: 'ADD_BLOCK_LINK', region_id: 'A-R016', link_id: 'human:1111',
    old_block_id: G20, new_block_id: T10, timestamp: '2026-09-24T11:30:00+00:00'})];

function setup({status = STATUS, reviews = [], edits = EDITS} = {}) {
    const calls = [];
    const json = (body, ok = true, code = 200) => Promise.resolve({ok, status: code, json: async () => body});
    const fetch = url => {
        calls.push(url);
        const path = new URL(url, 'http://portal.test').pathname;
        if (path.endsWith('/status')) return json(status);
        if (path.endsWith('/region-index')) return json(INDEX);
        if (path.endsWith('/reviews')) return json(reviews);
        if (path.endsWith('/block-links')) return json(edits);
        if (path.endsWith('/page-blocks')) return json({pages: []});
        if (path.endsWith('/ui-data')) return json({schema: 'human-mapping-ui-data/1', regions: []});
        return json({detail: 'not found'}, false, 404);
    };
    const sandbox = {console, setTimeout, URLSearchParams};
    sandbox.globalThis = sandbox;
    vm.runInNewContext(coreSource, sandbox);
    vm.runInNewContext(source, sandbox);
    const SBM = sandbox.StageBlockMapping;
    return {SBM, store: SBM.createStore({fetch, Vue: null, sessionStorage: null}), calls};
}
const texts = m => [...m.banners, ...m.context, ...m.hints].map(x => x.text);

describe('deep link format (MASTER §7)', () => {
    const {SBM} = setup();
    const q = s => new URLSearchParams(s);

    it('ignores a route without the block view: stage 2 behaves as before', () => {
        expect(SBM.parseDeepLink(q(''))).toBeNull();
        expect(SBM.parseDeepLink(q(`object=${OID}&session=${SID}&pair=${PID}`))).toBeNull();
        expect(SBM.parseDeepLink(q('view=pages'))).toBeNull();
    });

    it('parses object, session, pair, run or result, the row composition, region and link', () => {
        expect(SBM.parseDeepLink(q(`object=${OID}&session=${SID}&pair=${PID}&view=blocks&run=${RUN}&lp=20&rp=10,26`
            + '&region=A-R016&link=human:1111'))).toEqual({objectId: OID, sessionId: SID, pairId: PID,
            binding: {kind: 'LIVE', runId: RUN}, lp: [20], rp: [10, 26], region: 'A-R016', link: 'human:1111'});
        const snap = SBM.parseDeepLink(`#/stage-comparison?object=${OID}&session=${SID}&pair=${PID}&view=blocks&result=${SNAP}`);
        expect(snap).toMatchObject({binding: {kind: 'SNAP', resultId: SNAP}, lp: null, rp: null, region: '', link: ''});
        const oneSided = SBM.parseDeepLink(q(`object=${OID}&session=${SID}&pair=${PID}&view=blocks&lp=21&rp=`));
        expect([oneSided.lp, oneSided.rp]).toEqual([[21], []]);
    });

    it('rejects ids that fail the client safe-id check, a run with a result, or a live result id', () => {
        const base = `object=${OID}&session=${SID}&pair=${PID}&view=blocks`;
        for (const bad of [`object=../x&session=${SID}&pair=${PID}&view=blocks`, `${base.replace(SID, 'a b')}`,
            `${base}&run=${RUN}&result=${SNAP}`, `${base}&run=../../etc`, `${base}&run=run1`, `${base}&run=${RUN.toUpperCase()}`, `${base}&result=pcv3res_${'a'.repeat(32)}`,
            `${base}&lp=20;DROP`, `${base}&lp=0&rp=1`, `${base}&lp=&rp=`, `${base}&region=A R016`, `${base}&link=javascript:x`,
            'view=blocks']) {
            expect(SBM.parseDeepLink(q(bad)), bad).toEqual({error: 'Ссылка повреждена: параметры не распознаны.'});
        }
    });

    it('builds the link of a view that parses back to the same intent', () => {
        const intent = {objectId: OID, sessionId: SID, pairId: PID, binding: {kind: 'LIVE', runId: RUN}, lp: [20],
            rp: [26, 10], region: 'A-R016', link: 'human:1111'};
        const link = SBM.buildDeepLink(intent);
        expect(link).toBe(`#/stage-comparison?object=${OID}&session=${SID}&pair=${PID}&view=blocks&run=${RUN}`
            + '&lp=20&rp=10,26&region=A-R016&link=human:1111');
        expect(SBM.parseDeepLink(link)).toEqual({...intent, rp: [10, 26]});
        expect(SBM.buildDeepLink({objectId: OID, sessionId: SID, pairId: PID, binding: {kind: 'SNAP', resultId: SNAP}}))
            .toBe(`#/stage-comparison?object=${OID}&session=${SID}&pair=${PID}&view=blocks&result=${SNAP}`);
    });
});

describe('applying an entry to the loaded pair', () => {
    async function entry(intent, options) {
        const env = setup(options);
        await env.store.load({sessionId: SID, pairId: PID});
        const result = await env.store.applyIntent({sessionId: SID, pairId: PID, ...intent}, () => ROWS);
        return {...env, result, S: env.store.state, m: env.store.messages({rows: ROWS})};
    }

    it('opens exactly the row of the link, focuses the region and selects the link', async () => {
        const {result, S, store} = await entry({binding: {kind: 'LIVE', runId: OTHER}, lp: [20], rp: [10],
            region: 'A-R016', link: 'human:1111'});
        expect(result.row.key).toBe('explicit-2');
        expect(S.binding).toEqual({kind: 'LIVE', runId: OTHER});
        expect(S.bindingSource).toBe('explicit');
        expect(S.lens).toMatchObject({key: 'L20|R10', mode: 'pair', focus: 'A-R016'});
        expect(S.selection.link).toBe(G20 + '|' + T10);
        expect(S.navOpen).toBe(false);
        expect(S.blocksActive).toBe(true);
        expect(store.deepLink()).toBe(`#/stage-comparison?object=${OID}&session=${SID}&pair=${PID}&view=blocks&run=${OTHER}`
            + '&lp=20&rp=10&region=A-R016&link=human:1111');
    });

    it('a row of another composition: the nearest row by the first OLD page, with the message', async () => {
        const {result, m} = await entry({lp: [20], rp: [10, 26]});
        expect(result.row.key).toBe('explicit-2');
        expect(texts(m)).toContain('Пара листов из ссылки изменилась — показан ближайший вид.');
    });

    it('an unknown region: the row without that focus and the message', async () => {
        const {S, m} = await entry({lp: [20], rp: [10], region: 'A-R099'});
        expect(S.lens.focus).toBe('');
        expect(texts(m)).toContain('Регион A-R099 не найден в результате a631b49a.');
    });

    it('a run the pair no longer offers: failure 4 with the list to choose, never the current run silently', async () => {
        const missing = 'dead0000000000000000000000000000';
        const {S, m, calls} = await entry({binding: {kind: 'LIVE', runId: missing}, lp: [20], rp: [10]});
        expect(S.binding).toEqual({kind: 'LIVE', runId: missing});
        const banner = m.banners.find(b => b.key === 'fail4');
        expect(banner.text).toBe('Результат dead0000 недоступен или изменён вне системы. Решения в его истории не удалены.');
        expect(banner.options.map(o => o.id)).toEqual([RUN, OTHER, SNAP]);
        expect(m.banners.filter(b => b.text === banner.text)).toHaveLength(1);
        expect(calls.filter(u => u.includes(missing))).toEqual([]);
        const snap = await entry({binding: {kind: 'SNAP', resultId: 'pcv3snap_' + '0'.repeat(32)}});
        expect(snap.m.banners.find(b => b.key === 'fail4').text)
            .toBe('Результат pcv3snap_0000… недоступен или изменён вне системы. Решения в его истории не удалены.');
        expect(snap.calls.filter(u => u.includes('/api/human-mapping/'))).toHaveLength(2);   // only the initial history
    });

    it('catalog entry (C13): all regions open, first region without a decision, its first two-sided row', async () => {
        const focus = {pair_id: PID, source_run_id: RUN, result_source: 'LIVE_RUN', result_id: ''};
        const {result, S} = await entry({catalogFocus: focus});
        expect(S.binding).toEqual({kind: 'LIVE', runId: RUN});
        expect(result.row.key).toBe('explicit-0');                   // A-R001 touches OLD 5 ↔ NEW 4
        expect(S.lens).toMatchObject({key: 'L5|R4', mode: 'pair', focus: 'A-R001'});
        expect(S.navOpen).toBe(true);
        // A region with a decision is skipped; a region without a two-sided row opens in «Весь регион».
        const reviews = ['A-R001', 'A-R016'].map((id, i) => hm({review_id: 'r' + i, region_id: id, status: 'HUMAN_UNCERTAIN',
            old_block_ids: [id + ':OLD:' + (id === 'A-R001' ? 1 : 20)], new_block_ids: [id + ':NEW:' + (id === 'A-R001' ? 1 : 5)],
            timestamp: '2026-09-24T12:00:00+00:00'}));
        const next = await entry({catalogFocus: focus}, {reviews});
        expect(next.result.row).toBeNull();
        expect(next.S.lens).toMatchObject({mode: 'region', focus: 'A-R003'});
    });

    it('a sealed snapshot of the catalog binds as the snapshot', async () => {
        const {S, calls} = await entry({catalogFocus: {pair_id: PID, source_run_id: RUN, result_source: 'SEALED_SNAPSHOT', result_id: SNAP}});
        expect(S.binding).toEqual({kind: 'SNAP', resultId: SNAP});
        expect(calls.some(u => u.includes(`/ui-data?result_id=${SNAP}`))).toBe(true);
    });

    it('waits for the load of the linked pair and gives up on another pair', async () => {
        const env = setup();
        const pending = env.store.applyIntent({sessionId: SID, pairId: PID, lp: [20], rp: [10]}, () => ROWS);
        await env.store.load({sessionId: SID, pairId: PID});
        expect((await pending).row.key).toBe('explicit-2');
        const other = setup();
        await other.store.load({sessionId: SID, pairId: PID});
        const first = other.store.applyIntent({sessionId: SID, pairId: 'pOther'}, () => ROWS);
        const second = other.store.applyIntent({sessionId: SID, pairId: PID}, () => ROWS);
        expect(await first).toBeNull();
        expect(await second).not.toBeNull();
    });
});

describe('app.js wiring of the entries (additive)', () => {
    const entries = app.slice(app.indexOf('let scPendingRouteIntent = null;'), app.indexOf('        return {',
        app.indexOf('let scPendingRouteIntent = null;')));

    it('calls scApplyRouteIntent once, in the stage-comparison route only', () => {
        expect(app.match(/scApplyRouteIntent\(routeQuery\);/g)).toHaveLength(1);
        const branch = app.slice(app.indexOf("} else if (hash === '/stage-comparison') {"), app.indexOf("} else if (hash === '/') {"));
        expect(branch).toContain('scLoadObjects();\n                scApplyRouteIntent(routeQuery);');
    });

    it('keeps the pinned body of pcOpenCatalogEntry and adds the blocks branch at its end', () => {
        const start = app.indexOf('async function pcOpenCatalogEntry');
        const body = app.slice(start, app.indexOf('\n        }\n', start));
        expect(body.startsWith('async function pcOpenCatalogEntry(entry, options) {')).toBe(true);
        expect(body).toContain("scTab.value = 'diffs';\n                if (options?.target === 'blocks') await scOpenCatalogBlocks();\n            } catch (error) {");
        expect(body).toContain("result_id: entry.result_source === 'SEALED_SNAPSHOT' ? entry.provenance?.result_id || '' : '',");
        expect(body).not.toMatch(/production\/run|scRunProduction|scStartProduction/);
    });

    // Runs the entries block of app.js with fake refs: object → session → pair → stage 2 in the blocks mode.
    function runEntries({session = SID, pairs = [{id: PID}], parse}) {
        const log = [];
        // Like Vue: an object put into a ref comes back as a (stable) proxy, never as the raw object.
        const proxies = new WeakMap();
        const wrap = v => (v && typeof v === 'object'
            ? proxies.get(v) || (proxies.set(v, new Proxy(v, {})), proxies.get(v)) : v);
        const ref = initial => { let v = initial; return {get value() { return wrap(v); }, set value(n) { v = n; }}; };
        const ctx = {
            window: {StageBlockMapping: {parseDeepLink: parse}}, ref, nextTick: async () => {},
            scBlockStore: ref({noteLinkIssue: t => log.push(['issue', t]),
                applyIntent: async (intent, rowsOf) => { log.push(['apply', intent.pairId, rowsOf().length]); return {row: {key: 'r'}}; }}),
            currentObjectId: ref('other'), storeObjectId: id => log.push(['store', id]), objectsList: ref([{id: OID, name: 'Объект'}]),
            objectName: ref(''), scSession: ref({id: session}), scSessionLoading: ref(false), scObjectsLoading: ref(false),
            scPairs: ref(pairs), scPairLoading: ref(false), scActivePair: ref(null), scTab: ref('upload'),
            scSheetMapRows: ref([1, 2]), scStage2Mode: ref('pages'), pcCatalogFocus: ref(null),
            scOpenPair: async pair => { ctx.scActivePair.value = pair; log.push(['pair', pair.id]); },
            scOpenSheetMapRow: row => log.push(['row', row.key]),
            pcWaitFor: async ready => ready(),
        };
        vm.createContext(ctx);
        vm.runInContext(entries + ';this.apply = scApplyRouteIntent; this.catalog = scOpenCatalogBlocks;', ctx);
        return {ctx, log};
    }
    const INTENT = {objectId: OID, sessionId: SID, pairId: PID, binding: null, lp: [20], rp: [10], region: '', link: ''};
    const settle = () => new Promise(resolve => setTimeout(resolve, 0));

    it('a valid link switches the object, opens the pair and stage 2 in the blocks mode', async () => {
        const {ctx, log} = runEntries({parse: () => INTENT});
        ctx.apply(new URLSearchParams('view=blocks'));
        await settle();
        expect(ctx.currentObjectId.value).toBe(OID);
        expect(log).toEqual([['store', OID], ['pair', PID], ['apply', PID, 2], ['row', 'r']]);
        expect([ctx.scTab.value, ctx.scStage2Mode.value]).toEqual(['links', 'blocks']);
    });

    it('a broken link, a foreign session or a missing pair only leave a notice', async () => {
        let env = runEntries({parse: () => ({error: 'Ссылка повреждена: параметры не распознаны.'})});
        env.ctx.apply(new URLSearchParams(''));
        expect(env.log).toEqual([['issue', 'Ссылка повреждена: параметры не распознаны.']]);
        expect(env.ctx.currentObjectId.value).toBe('other');
        env = runEntries({parse: () => INTENT, session: 'another'});
        env.ctx.apply(new URLSearchParams(''));
        await settle();
        expect(env.log.at(-1)).toEqual(['issue', 'Сессия сравнения этого результата не открылась для объекта.']);
        expect(env.ctx.scStage2Mode.value).toBe('pages');
        env = runEntries({parse: () => INTENT, pairs: []});
        env.ctx.apply(new URLSearchParams(''));
        await settle();
        expect(env.log.at(-1)).toEqual(['issue', 'Пара документов результата не найдена в сессии объекта.']);
        env = runEntries({parse: () => null});
        env.ctx.apply(new URLSearchParams(''));
        expect(env.log).toEqual([]);
    });

    it('the catalog branch opens the same pair with the catalog focus', async () => {
        const {ctx, log} = runEntries({parse: () => null});
        ctx.scActivePair.value = {id: PID};
        ctx.pcCatalogFocus.value = {pair_id: PID, source_run_id: RUN};
        await ctx.catalog();
        expect(log).toEqual([['apply', PID, 2], ['row', 'r']]);
        expect(ctx.scStage2Mode.value).toBe('blocks');
    });

    it('adds no API URL and no forbidden token to app.js', () => {
        expect(entries).not.toMatch(/\/api\//);
        for (const token of ['semantic-diff', 'page-image', 'block-image', 'change-regions']) expect(app).not.toContain(token);
    });
});

describe('markup of the entries', () => {
    it('adds «Смысловые блоки →» to the catalog focus banner and the link notice to the stage shell', () => {
        const focus = html.slice(html.indexOf('id="pc-catalog-focus"'), html.indexOf('</div>', html.indexOf('id="pc-catalog-focus"')));
        expect(focus).toContain('<button v-if="scBlockStore" type="button" class="btn btn-sm btn-secondary" @click="scOpenCatalogBlocks()">Смысловые блоки →</button>');
        expect(focus).toContain('Показать все результаты пары');
        expect(html).toContain('<sbm-link-notice v-if="scBlockStore" :store="scBlockStore"></sbm-link-notice>');
        expect(html.indexOf('<sbm-link-notice')).toBeLessThan(html.indexOf('<project-comparison-catalog'));
    });

    it('copies the link with the clipboard and falls back to a selected field', () => {
        const copy = source.slice(source.indexOf('async function copyLink()'), source.indexOf('function stepPair('));
        expect(copy).toContain('try {\n                        await root.navigator.clipboard.writeText(text);');
        expect(copy).toContain("copied.value = 'manual';");
        expect(source).toContain('{{ T.COPY_LINK }}');
        expect(source).toContain("COPY_LINK: 'Скопировать ссылку',");
    });
});

describe('narrow layouts never scroll the page sideways (UNIFIED_UX_DESIGN §13)', () => {
    const css = read('../static/css/stage-block-mapping.css').replace(/\/\*[\s\S]*?\*\//g, '');
    const rule = selector => {
        const at = css.indexOf(selector);
        return at < 0 ? '' : css.slice(at, css.indexOf('}', at));
    };

    it('lets the segment and the lens shrink and wrap inside the viewer toolbar', () => {
        expect(rule('.sbm-segment, .sbm-lens {')).toMatch(/flex: 0 1 auto;[\s\S]*min-width: 0;[\s\S]*max-width: 100%;/);
        expect(rule('.sbm-segment button, .sbm-lens button {')).toMatch(/white-space: normal;[\s\S]*overflow-wrap: anywhere;/);
    });

    it('wraps every button and chip of the compact / single / list layouts', () => {
        for (const layout of ['compact', 'single', 'list']) {
            const buttons = rule(`.sbm-layout--${layout} button`);
            expect(buttons, layout).toContain('max-width: 100%;');
            expect(buttons, layout).toContain('white-space: normal;');
            expect(buttons, layout).toContain('overflow-wrap: anywhere;');
            expect(css).toContain(`.sbm-layout--${layout} .sbm-region`);
        }
    });
});

describe('catalog button «Смысловые блоки в шаге 2» (C13)', () => {
    function load() {
        const sandbox = {Vue: {ref: value => ({value}), computed: fn => ({get value() { return fn(); }}), onMounted: () => {}},
            fetch: async () => ({ok: true, json: async () => ({})}), open: () => {},
            localStorage: {getItem: () => null, setItem: () => {}}};
        sandbox.globalThis = sandbox;
        vm.runInNewContext(catalogSource, sandbox);
        let definition;
        sandbox.ProjectComparisonCatalog.register({component: (name, def) => { definition = def; }});
        return definition;
    }

    it('emits open with {target: "blocks"}; emits stays ["open"]; «Открыть» unchanged', () => {
        const definition = load();
        expect(definition.emits).toEqual(['open']);
        const emitted = [];
        const state = definition.setup({}, {emit: (...args) => emitted.push(args)});
        const entry = {catalog_entry_id: 'e1'};
        state.open(entry);
        state.open(entry, {target: 'blocks'});
        expect(emitted).toEqual([['open', entry], ['open', entry, {target: 'blocks'}]]);
    });

    it('is disabled exactly when «Открыть» is, with the same hint', () => {
        const template = load().template;
        const button = template.slice(template.lastIndexOf('<button', template.indexOf('>Смысловые блоки в шаге 2</button>')),
            template.indexOf('>Смысловые блоки в шаге 2</button>'));
        expect(button).toContain(':disabled="opening || !entry.open.available"');
        expect(button).toContain("'В ленте пары сейчас показан другой прогон'");
        expect(button).toContain("@click=\"open(entry, {target: 'blocks'})\"");
        expect(template).toContain("@click=\"open(entry)\">Открыть</button>");
        expect(template).toContain('@click="openHumanMapping(entry)">Human Mapping');
    });
});

describe('П-18: Human Mapping link of the change list has no empty scope', () => {
    const require = createRequire(import.meta.url);
    const V = require('../static/js/project-change-view.js');
    const context = vm.createContext({ProjectChangeView: V});
    vm.runInContext(read('../static/js/vue.global.prod.js'), context);
    vm.runInContext(read('../static/js/project-change-ui.js'), context);
    let component;
    context.ProjectChangeUI.register({component: (_, c) => { component = c; }});
    const href = change => component.setup(context.Vue.reactive({changes: [change], pairs: [{id: PID}], selectedPairId: PID,
        report: false, readonly: true, objectId: OID, resultHref: ''}), {emit: () => {}}).humanMappingHref.value;
    const card = extra => ({id: 'c1', pair_ids: [PID], evidence: [], conflicts: [], details: [], technical_provenance: [],
        candidate_version: 'projectchange_v3', source_run_id: RUN, ...extra});

    it('omits session_id and run_id when the card has no session (sealed snapshot)', () => {
        expect(href(card({session_id: SID}))).toContain('&run_id=');       // the card is in the opened pair
        expect(href(card({session_id: ''}))).toBe(`/human-mapping/?object=${OID}&comparison=${PID}`);
        expect(href(card({session_id: undefined}))).toBe(`/human-mapping/?object=${OID}&comparison=${PID}`);
    });

    it('keeps the explicit scope of a live run', () => {
        expect(href(card({session_id: SID}))).toBe(`/human-mapping/?object=${OID}&comparison=${PID}&session_id=${SID}&run_id=${RUN}`);
    });
});
