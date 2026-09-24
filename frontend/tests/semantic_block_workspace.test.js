// Stage 2 «Смысловые блоки» (phase B, view only): the store of stage-block-mapping.js on top of
// human-mapping-core.js, run in node:vm with a fake fetch — no network, no DOM, no Vue.
// Fixtures are synthetic but DEV5-shaped: region pages copied from the frozen run a631b49a of the
// pair p290a06df79 (read-only copy), shared members as in the real ui_data, 7 saved sheet links.
import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const coreSource = readFileSync(new URL('../static/js/human-mapping-core.js', import.meta.url), 'utf8');
const source = readFileSync(new URL('../static/js/stage-block-mapping.js', import.meta.url), 'utf8');

const SID = 'e6fc8a2725eb4a67', PID = 'p290a06df79', OID = '4f3e5916';
const RUN = 'a631b49aaaac4db0af66a495c155c629', NEXT = '7f3c21aa0000000000000000000000ff';
const FAILED = 'bad0000000000000000000000000000f';
const SNAP = 'pcv3snap_5157938d548fb1df6fbcf87c200c1347';
const local = (month, day, hour, minute) => new Date(2026, month - 1, day, hour, minute).toISOString();

// DEV5 region pages [OLD, NEW] (ui_data.regions[].pages of run a631b49a).
const PAGES = {
    'A-R001': [[1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 6, 11]], 'A-R002': [[6, 7, 8], [4, 7, 12, 13, 14]],
    'A-R003': [[9, 15], [15, 16, 23]], 'A-R004': [[10, 11], [8, 16, 17]], 'A-R005': [[12, 13, 14], [20, 21, 22]],
    'A-R006': [[16], [10, 26]], 'A-R007': [[17], [27]], 'A-R008': [[18], [24]], 'A-R009': [[19], [25]],
    'A-R010': [[11, 16, 19, 20], [9, 19, 20, 28, 30]], 'A-R011': [[7, 12, 21], [10, 13, 21, 29]],
    'A-R012': [[20, 22, 23, 24, 25, 26, 27, 29, 30, 31], [8, 31, 32, 33, 34]],
    'A-R013': [[32, 33, 34, 35, 36, 38, 39, 40], [9, 39, 40, 41, 42, 43, 44, 45, 46]],
    'A-R014': [[12, 20], [9, 17, 18, 26, 35, 36, 37, 38, 47, 48, 49, 50, 51, 52, 53]],
    'A-R015': [[41, 44, 45, 48, 50], [54, 57, 58, 61, 63]], 'A-R016': [[20, 51, 52], [5, 10, 26]],
};
// Real shared members: GRAPHIC OLD 20, TABLE NEW 10 and TABLE NEW 9 of the DEV5 run.
const G20 = 'blk_d2ebac1ee4424ad596bcbe459b211e88', T10 = 'blk_55bd5531', T9 = 'blk_ab9a0461';
const SHARED = [['OLD', 20, G20, ['A-R010', 'A-R012', 'A-R014', 'A-R016'], 'GRAPHIC'],
    ['NEW', 10, T10, ['A-R006', 'A-R011', 'A-R016'], 'TABLE'], ['NEW', 9, T9, ['A-R010', 'A-R014'], 'TABLE']];
function memberId(region, side, page) {
    const hit = SHARED.find(([s, p, , regions]) => s === side && p === page && regions.includes(region));
    return hit ? {id: hit[2], type: hit[4]} : {id: `${region}:${side}:${page}`, type: 'TEXT'};
}
function indexRegion(id, [olds, news], overrides = {}) {
    const members = {OLD: olds.map(p => ({...memberId(id, 'OLD', p), page: p})),
        NEW: news.map(p => ({...memberId(id, 'NEW', p), page: p}))};
    return {id, title: id + ' домен', domain: id + ' домен', scope: '', confidence: 0.9,
        membership_state: {OLD: 'MAPPED', NEW: 'MAPPED'}, mapping_state: 'MAPPED',
        member_cardinality: members.OLD.length === 1 && members.NEW.length === 1 ? '1:1'
            : members.OLD.length === 1 ? '1:N' : members.NEW.length === 1 ? 'N:1' : 'N:N',
        pages: {OLD: olds, NEW: news}, members,
        allowed: {OLD: members.OLD.map(b => b.id), NEW: members.NEW.map(b => b.id)}, invalid_ref_count: 0, ...overrides};
}
const INDEX = {schema: 'stage-block-mapping-region-index/1', session_id: SID, pair_id: PID, object_id: OID, run_id: RUN,
    regions: Object.entries(PAGES).map(([id, pages]) => indexRegion(id, pages))};

const SAVED = [[5, 4], [6, 12], [7, 13], [17, 7], [18, 8], [19, 9], [20, 10]];
const LINKS = SAVED.map(([l, r]) => ({left_pages: [l], right_pages: [r]}));
const ROWS = [
    ...SAVED.map(([l, r], i) => ({key: 'explicit-' + i, leftPages: [l], rightPages: [r], source: 'auto',
        confidence: 'high', explicitLinkIndex: i})),
    {key: 'left-only-21', leftPages: [21], rightPages: [], source: 'unmatched', confidence: 'unmatched', explicitLinkIndex: null},
    {key: 'right-only-26', leftPages: [], rightPages: [26], source: 'unmatched', confidence: 'unmatched', explicitLinkIndex: null},
];
const ROW_20_10 = ROWS[6], ROW_21 = ROWS[7];
const SHEETS = {OLD: {20: '5', 21: '6'}, NEW: {10: '5'}};
const sheetOf = (side, page) => (SHEETS[side] || {})[page] || '';

function runEntry(overrides = {}) {
    return {run_id: RUN, state: 'COMPLETED_FROZEN', frozen: true, is_current: true, created_at: local(9, 21, 10, 0),
        completed_at: local(9, 21, 12, 0), engine_version: '3.5.2', provider: 'claude_code_cli_subscription',
        model: 'claude-opus-5', model_display: 'Claude Opus 5', reasoning: 'xhigh', projectchange_count: 64,
        region_count: 16, hm_available: true, hm_reason: null, pdf_match: true, source_stale: false,
        blocks_content_match: null, history: {reviews: 0, block_link_events: 0, poisoned: false, poison_codes: []},
        writable: false, write_block_reason: 'WRITES_DISABLED', write_block_reasons: ['WRITES_DISABLED'], ...overrides};
}
function statusBody(overrides = {}) {
    return {schema: 'stage-block-mapping-status/1', session_id: SID, pair_id: PID, object_id: OID, object_error: null,
        generated_at: local(9, 24, 12, 0), current_run_id: RUN,
        latest_attempt: {run_id: RUN, state: 'COMPLETED_FROZEN', reason_code: 'v3_completed', created_at: local(9, 21, 10, 0),
            completed_at: local(9, 21, 12, 0)},
        runs: [runEntry()], snapshots: [],
        source_blocks: {OLD: {available: true, pdf_sha256: '12b3ecea9a843fa9fb3cfabbbfed03cc0f3229ef197add90be1eaa4e80a5f757'},
            NEW: {available: true, pdf_sha256: '9340ca55017eac3b17c2d99abb83f845da685c75af024e33e77755292ab35a9e'}},
        capabilities: {block_mapping_writes: false}, ...overrides};
}
const hmRow = (extra) => ({object_id: OID, pair_key: PID, comparison_id: PID, pair: PID, run_id: RUN, result_id: null,
    comment: '', ...extra});
const add = (event_id, region_id, link_id, old_block_id, new_block_id, timestamp) =>
    hmRow({event_id, event_type: 'ADD_BLOCK_LINK', region_id, link_id, old_block_id, new_block_id, timestamp});
const review = (review_id, region_id, status, old_block_ids, new_block_ids, timestamp) =>
    hmRow({review_id, region_id, status, old_block_ids, new_block_ids, timestamp, reviewer_source: 'HUMAN'});
const t = minute => `2026-09-24T11:${String(minute).padStart(2, '0')}:00.000000+00:00`;
// Anchor in A-R016 (GRAPHIC 20 → TABLE 10); the same edge GRAPHIC 20 → TABLE 9 confirmed in A-R010 and rejected in A-R014.
const DECIDED = {
    edits: [add('e1', 'A-R016', 'human:1', G20, T10, t(30)), add('e2', 'A-R010', 'human:2', G20, T9, t(31)),
        add('e3', 'A-R014', 'human:3', G20, T9, t(32))],
    reviews: [review('r1', 'A-R016', 'HUMAN_CONFIRMED', [G20], [T10], t(39)),
        review('r2', 'A-R010', 'HUMAN_CONFIRMED', [G20], [T9], t(40)),
        review('r3', 'A-R014', 'HUMAN_REJECTED', [G20], [T9], t(41))],
};

function pageBlocks(side, pages, layer = 'RUN') {
    return {schema: 'stage-block-mapping-page-blocks/1', layer, side, pages: pages.map(page => ({physical_page: page,
        available: true, reason: null, geometry: {width_px: 1800, height_px: 1274, rotation: null, geometry_source: 'X'},
        blocks: side === 'OLD' && page === 20
            ? [{block_id: G20, modality: 'GRAPHIC', source_block_type: 'image', bbox: [0.3, 0.01, 1, 0.8]},
                {block_id: 'blk_4da00ad8', modality: 'TEXT', source_block_type: 'text', bbox: [0.1, 0.54, 0.29, 0.94]},
                {block_id: 'blk_c1bddb8c', modality: 'TEXT', source_block_type: 'stamp', bbox: [0.68, 0.86, 0.99, 0.99]}]
            : [{block_id: `${side}-${page}-a`, modality: 'TABLE', source_block_type: 'text', bbox: [0.1, 0.02, 0.98, 0.62]},
                {block_id: `${side}-${page}-s`, modality: 'TEXT', source_block_type: 'stamp', bbox: [0.09, 0.92, 0.97, 0.98]}]}))};
}

const BRIDGE_OK = {ok: true, cached: false, checked_at: local(9, 24, 14, 52),
    result: {confirmed_anchor_count: 2, rejected_link_count: 1, review_snapshot_sha256: 'f'.repeat(64)}};
function setup({status = statusBody(), index = INDEX, reviews = [], edits = [], ui = null, fail = {}, storage,
    bridge = BRIDGE_OK} = {}) {
    const calls = [];
    const state = {status, index, reviews, edits, ui, bridge};
    const json = (body, ok = true, code = 200) => Promise.resolve({ok, status: code, json: async () => body});
    const fetch = url => {
        calls.push(url);
        const u = new URL(url, 'http://portal.test');
        for (const [pattern, body] of Object.entries(fail)) if (u.pathname.includes(pattern)) {
            return body === 'reject' ? Promise.reject(new TypeError('network')) : json(body.body, false, body.code);
        }
        if (u.pathname.endsWith('/block-mapping/status')) return json(state.status);
        if (u.pathname.endsWith('/region-index')) return json(state.index);
        if (u.pathname.endsWith('/page-blocks')) {
            return json(pageBlocks(u.searchParams.get('side'), u.searchParams.get('pages').split(',').map(Number),
                u.pathname.includes('/source/') ? 'SOURCE' : 'RUN'));
        }
        if (u.pathname.includes('/blocks/')) return json({schema: 'stage-block-mapping-block/1', modality: 'TEXT', structured_md: 'текст', tables: []});
        if (u.pathname.endsWith('/bridge-check')) return json(state.bridge);
        if (u.pathname.endsWith('/reviews')) return json(state.reviews);
        if (u.pathname.endsWith('/block-links')) return json(state.edits);
        if (u.pathname.endsWith('/ui-data')) return json(state.ui);
        return json({detail: 'not found'}, false, 404);
    };
    const sandbox = {console};
    sandbox.globalThis = sandbox;
    vm.runInNewContext(coreSource, sandbox);
    vm.runInNewContext(source, sandbox);
    const SBM = sandbox.StageBlockMapping;
    const store = SBM.createStore({fetch, Vue: null, sessionStorage: storage === undefined ? null : storage});
    return {SBM, store, calls, state, S: store.state};
}
const flush = () => new Promise(resolve => setImmediate(resolve));
const texts = m => [...m.banners, m.conflict, ...m.context, ...m.hints, ...m.footer].filter(Boolean).map(x => x.text);
const kind = url => {
    const path = new URL(url, 'http://portal.test').pathname;
    for (const k of ['status', 'region-index', 'page-blocks', 'bridge-check', 'ui-data', 'reviews', 'block-links', 'page-preview', 'assets'])
        if (path.includes(k)) return k;
    return path.includes('/blocks/') ? 'blocks' : path;
};
const count = (calls, k) => calls.filter(u => kind(u) === k).length;

describe('P1 request budget (C6)', () => {
    it('makes no request before a pair is selected', async () => {
        const {store, calls} = setup();
        await store.load({});
        await store.load({sessionId: SID});
        expect(calls).toEqual([]);
        expect(store.rowChip(ROW_20_10)).toBeNull();
    });

    it('after selection: one status and, with a binding, one index, reviews and block-links — nothing else', async () => {
        const {store, calls} = setup();
        await store.load({sessionId: SID, pairId: PID});
        expect(calls.map(kind).sort()).toEqual(['block-links', 'region-index', 'reviews', 'status']);
        // Explicit scope on every HM call; object from status.object_id.
        expect(calls.find(u => kind(u) === 'reviews'))
            .toBe(`/api/human-mapping/objects/${OID}/comparisons/${PID}/reviews?session_id=${SID}&run_id=${RUN}`);
        expect(calls.find(u => kind(u) === 'block-links'))
            .toBe(`/api/human-mapping/objects/${OID}/comparisons/${PID}/block-links?session_id=${SID}&run_id=${RUN}`);
        // Chips of the «Листы» mode need no further request.
        for (const row of ROWS) store.rowChip(row);
        store.summary();
        store.messages({rows: ROWS, sheetOf});
        expect(calls).toHaveLength(4);
        for (const k of ['page-blocks', 'blocks', 'bridge-check', 'ui-data', 'assets']) expect(count(calls, k)).toBe(0);
    });

    it('without a binding only status is asked (state F)', async () => {
        const {store, calls} = setup({status: statusBody({current_run_id: null, runs: [], latest_attempt: null})});
        await store.load({sessionId: SID, pairId: PID});
        expect(calls.map(kind)).toEqual(['status']);
    });

    it('loads page blocks of the lens only in the blocks mode; rasters from the stage-2 preview when pdf_match', async () => {
        const {store, calls, S} = setup();
        await store.load({sessionId: SID, pairId: PID});
        store.openRow(ROW_20_10);
        expect(count(calls, 'page-blocks')).toBe(0);
        store.enterBlocks(ROW_20_10);
        await flush();
        const pageCalls = calls.filter(u => kind(u) === 'page-blocks');
        expect(pageCalls).toEqual([
            `/api/stage-comparison/sessions/${SID}/pairs/${PID}/block-mapping/runs/${RUN}/page-blocks?side=OLD&pages=20`,
            `/api/stage-comparison/sessions/${SID}/pairs/${PID}/block-mapping/runs/${RUN}/page-blocks?side=NEW&pages=10`]);
        // Without the viewer's signature: fallback key v=<pdf_sha256[:12]>, single page → width 1400.
        expect(store.rasterUrl('OLD', 20))
            .toBe(`/api/stage-comparison/sessions/${SID}/pairs/${PID}/page-preview?side=left&page=20&width=1400&v=12b3ecea9a84`);
        expect(store.rasterUrl('NEW', 10)).toContain('side=right&page=10&width=1400&v=9340ca55017e');
        expect(store.rasterUrl('NEW', 10)).not.toContain('/api/human-mapping/');
        // Geometry comes with the blocks, so the page box has its proportions before the image decodes.
        expect(store.pageView('OLD', 20).geometry).toMatchObject({width_px: 1800, height_px: 1274});
        expect(count(calls, 'bridge-check') + count(calls, 'ui-data') + count(calls, 'blocks')).toBe(0);
        // The same lens again: cached, no new request; block text only on click.
        store.enterBlocks(ROW_20_10);
        await flush();
        expect(count(calls, 'page-blocks')).toBe(2);
        store.selectBlock('OLD', G20, 20);
        await flush();
        expect(calls.at(-1)).toBe(`/api/stage-comparison/sessions/${SID}/pairs/${PID}/block-mapping/runs/${RUN}/blocks/OLD/${G20}?page=20`);
        expect(S.selection.OLD).toEqual([G20]);
        expect(store.detail('OLD', G20).status).toBe('ok');
    });
});

describe('page rasters share the browser cache of the sheet viewer (PERFORMANCE_PLAN §4.2)', () => {
    // The real scPagePreviewUrl of app.js, run with the viewer's base URL of this pair.
    const appSource = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
    const start = appSource.indexOf('function scPagePreviewUrl(side, page, width, signature)');
    const fnSource = appSource.slice(start, appSource.indexOf('\n        }\n', start) + 10);
    const viewer = vm.runInNewContext(`${fnSource}; scPagePreviewUrl`, {encodeURIComponent,
        scPageApiBase: () => `/api/stage-comparison/sessions/${SID}/pairs/${PID}`});
    const signatures = {left: '1726900000123456789:27301234', right: '1726900000987654321:13002345'};
    const builder = (side, page, width) => (signatures[side] ? viewer(side, page, width, signatures[side]) : '');
    const W = {single: appSource.match(/const SC_PREVIEW_WIDTH = (\d+);/)[1],
        stack: appSource.match(/const SC_CONTINUOUS_PREVIEW_WIDTH = (\d+);/)[1]};

    it('builds byte-identical URLs with the viewer signature: 1400 for one page, 1000 for a stack', async () => {
        expect(W).toEqual({single: '1400', stack: '1000'});
        const {store} = setup();
        await store.load({sessionId: SID, pairId: PID});
        store.setPreviewUrlBuilder(builder);
        store.openRow(ROW_20_10);
        store.enterBlocks(ROW_20_10);
        expect(store.rasterUrl('OLD', 20)).toBe(viewer('left', 20, 1400, signatures.left));
        expect(store.rasterUrl('NEW', 10)).toBe(viewer('right', 10, 1400, signatures.right));
        expect(store.rasterUrl('OLD', 20)).toBe(`/api/stage-comparison/sessions/${SID}/pairs/${PID}/page-preview?side=left&page=20`
            + '&width=1400&v=1726900000123456789%3A27301234');
        store.setLens('region');                          // A-R016: OLD 20, 51, 52 · NEW 5, 10, 26 — stacks
        expect(store.rasterUrl('OLD', 51)).toBe(viewer('left', 51, 1000, signatures.left));
        expect(store.rasterUrl('NEW', 26)).toBe(viewer('right', 26, 1000, signatures.right));
    });

    it('keeps a built URL while the viewer reloads, falls back to v=<sha[:12]> otherwise; K-PDF unchanged', async () => {
        const {store} = setup();
        await store.load({sessionId: SID, pairId: PID});
        store.setPreviewUrlBuilder(builder);
        store.openRow(ROW_20_10);
        const shared = store.rasterUrl('OLD', 20);
        signatures.left = '';
        expect(store.rasterUrl('OLD', 20)).toBe(shared);      // no flicker to another cache key
        expect(store.rasterUrl('OLD', 21)).toContain('&width=1400&v=12b3ecea9a84');
        store.setPreviewUrlBuilder(() => { throw new Error('viewer not ready'); });
        expect(store.rasterUrl('NEW', 10)).toContain('&v=9340ca55017e');
        const kpdf = setup({status: statusBody({runs: [runEntry({pdf_match: false, write_block_reason: 'SOURCE_PDF_CHANGED'})]})});
        await kpdf.store.load({sessionId: SID, pairId: PID});
        kpdf.store.setPreviewUrlBuilder(builder);
        kpdf.store.openRow(ROW_20_10);
        expect(kpdf.store.rasterUrl('OLD', 20))
            .toBe(`/api/human-mapping/objects/${OID}/comparisons/${PID}/assets/old/p020/full_page.png?session_id=${SID}&run_id=${RUN}`);
    });

    it('waits (no URL, no fallback key) while the viewer reads page-info, then loads once with the signature', async () => {
        const {store} = setup();
        await store.load({sessionId: SID, pairId: PID});
        const viewerState = {signature: '', reading: true};
        store.setPreviewUrlBuilder((side, page, width) => (viewerState.signature
            ? viewer(side, page, width, viewerState.signature) : viewerState.reading ? null : ''));
        store.openRow(ROW_20_10);
        expect(store.rasterUrl('OLD', 20)).toBe('');           // the <img> is not rendered yet: blocks come from geometry
        viewerState.signature = signatures.right || '1726900000987654321:13002345';
        expect(store.rasterUrl('OLD', 20)).toBe(viewer('left', 20, 1400, viewerState.signature));
        viewerState.signature = '';
        viewerState.reading = false;                            // e.g. one-sided row: the viewer never reads this side
        expect(store.rasterUrl('NEW', 10)).toContain('&width=1400&v=9340ca55017e');
    });
});

describe('binding order (D-6, C13)', () => {
    it('catalog focus of this pair wins over the pointer; another pair\'s focus is ignored', async () => {
        const other = runEntry({run_id: NEXT, is_current: false});
        const status = statusBody({runs: [runEntry(), other]});
        let env = setup({status});
        await env.store.load({sessionId: SID, pairId: PID, catalogFocus: {pair_id: PID, source_run_id: NEXT}});
        expect(env.S.binding).toEqual({kind: 'LIVE', runId: NEXT});
        expect(env.S.bindingSource).toBe('catalog');
        env = setup({status});
        await env.store.load({sessionId: SID, pairId: PID, catalogFocus: {pair_id: 'pOther', source_run_id: NEXT}});
        expect(env.S.binding).toEqual({kind: 'LIVE', runId: RUN});
        expect(env.S.bindingSource).toBe('current');
    });

    it('uses result_id only for a SEALED_SNAPSHOT entry (a LIVE_RUN pcv3res_ id would 404 in HM)', () => {
        const {SBM} = setup();
        const live = SBM.chooseBinding(statusBody(), {pair_id: PID, source_run_id: RUN, result_id: 'pcv3res_' + 'a'.repeat(32),
            result_source: 'LIVE_RUN'}, PID);
        expect(live.binding).toEqual({kind: 'LIVE', runId: RUN});
        const snap = SBM.chooseBinding(statusBody(), {pair_id: PID, result_id: SNAP, result_source: 'SEALED_SNAPSHOT'}, PID);
        expect(snap.binding).toEqual({kind: 'SNAP', resultId: SNAP});
        const noSource = SBM.chooseBinding(statusBody(), {pair_id: PID, result_id: SNAP}, PID);
        expect(noSource.binding).toEqual({kind: 'LIVE', runId: RUN});
        expect(SBM.chooseBinding(statusBody({current_run_id: null}), null, PID).binding).toBeNull();
        // A sealed snapshot's source_run_id is the run it was cut from — never a LIVE binding of this pair.
        const sealedNoId = SBM.chooseBinding(statusBody({current_run_id: null}),
            {pair_id: PID, source_run_id: 'projectchange_elsewhere', result_source: 'SEALED_SNAPSHOT', result_id: ''}, PID);
        expect(sealedNoId.binding).toBeNull();
    });

    it('a snapshot binding loads ui-data, reviews and block-links (with ?result_id) only in the blocks mode', async () => {
        const ui = {schema: 'human-mapping-ui-data/1', regions: [{id: 'A-R001', domain: 'Состав', scope: '',
            old_blocks: [{id: 'o1', type: 'TEXT', page: 1, bbox: [0, 0, 0.5, 0.5]}],
            new_blocks: [{id: 'n1', type: 'TEXT', page: 1, bbox: [0, 0, 0.5, 0.5]}],
            pages: {OLD: [{page: 1, image: 'x', blocks: [{id: 'o1', type: 'TEXT', bbox: [0, 0, 0.5, 0.5]}]}],
                NEW: [{page: 1, image: 'x', blocks: [{id: 'n1', type: 'TEXT', bbox: [0, 0, 0.5, 0.5]}]}]},
            membership_state: {OLD: 'MAPPED', NEW: 'MAPPED'}}]};
        const legacy = [review('lr1', 'A-R001', 'HUMAN_CONFIRMED', ['o1'], ['n1'], t(39)),
            review('lr2', 'A-R001', 'HUMAN_CONFIRMED', ['o1'], ['n1'], t(40))].map(r => ({...r, run_id: null, result_id: SNAP}));
        const status = statusBody({snapshots: [{result_id: SNAP, catalog_entry_id: 'c', label: 'АР1', hm_available: true,
            hm_url: `/human-mapping/?object=${OID}&comparison=${PID}&result_id=${SNAP}`,
            legacy_history: {reviews_total: 2, reviews_this_result: 2, block_link_events_total: 0, block_link_events_this_result: 0},
            writable: false, write_block_reason: 'SNAPSHOT_READ_ONLY'}]});
        const {store, calls, S} = setup({status, ui, reviews: legacy});
        await store.load({sessionId: SID, pairId: PID, catalogFocus: {pair_id: PID, result_id: SNAP, result_source: 'SEALED_SNAPSHOT'}});
        expect(calls.map(kind)).toEqual(['status']);
        expect(store.rowChip(ROWS[0])).toBeNull();          // no snapshot index before the blocks mode (C6)
        store.enterBlocks({key: 'r', leftPages: [1], rightPages: [1], explicitLinkIndex: 0});
        await flush();
        await flush();
        expect(calls.filter(u => ['ui-data', 'reviews', 'block-links'].includes(kind(u))).sort()).toEqual([
            `/api/human-mapping/objects/${OID}/comparisons/${PID}/block-links?result_id=${SNAP}`,
            `/api/human-mapping/objects/${OID}/comparisons/${PID}/reviews?result_id=${SNAP}`,
            `/api/human-mapping/objects/${OID}/comparisons/${PID}/ui-data?result_id=${SNAP}`]);
        expect(count(calls, 'region-index')).toBe(0);
        expect(count(calls, 'assets')).toBe(0);
        // G-SNAP texts; legacy history: latest = second row, the first is shown grey (superseded).
        const m = store.messages({rows: ROWS, sheetOf});
        expect(texts(m)).toContain('Решения этого снимка хранятся отдельно от результатов анализа и не могут стать якорями нового анализа.');
        expect(texts(m)).toContain('Снимок pcv3snap_5157… — только просмотр.');
        expect(m.banners[0].actions[0].href).toBe(`/human-mapping/?object=${OID}&comparison=${PID}&result_id=${SNAP}`);
        expect(store.bindingChip()).toBe('Снимок pcv3snap_5157… · только просмотр ▾');
        const journal = store.journal('A-R001');
        expect(journal).toHaveLength(2);
        expect(journal[0].superseded).toMatch(/^заменено /);
        expect(journal[1].superseded).toBe('');
        expect(store.model().latest.get('A-R001').review_id).toBe('lr2');
        expect(S.binding).toEqual({kind: 'SNAP', resultId: SNAP});
    });
});

describe('state detection → exact texts (UX_STATE_MATRIX §1, §3, §4)', () => {
    async function view({status = statusBody(), row = ROW_20_10, mapBuilt = true, ...rest} = {}) {
        const env = setup({status, ...rest});
        await env.store.load({sessionId: SID, pairId: PID});
        if (row) env.store.openRow(row);
        return {...env, m: env.store.messages({rows: ROWS, mapBuilt, sheetOf})};
    }

    it('A + H + L on DEV5 (row OLD 20 ↔ NEW 10, N↔N region without AI links, writes disabled)', async () => {
        const {m, store} = await view();
        expect(texts(m)).toEqual(expect.arrayContaining([
            'Редактирование смысловых связей в этом разделе ещё не включено. Для правки откройте Human Mapping ↗',
            'В регионе A-R016 (N↔N) ИИ не предложил пар блоков. Выделите блок слева и справа и соедините их.',
            'Решения по блокам не меняют полученный результат анализа (64 изменения).',
            'Решения записываются в историю прогона a631b49a. Результат «Изменения проекта» этого прогона заморожен и не меняется. Подтверждённые связи станут якорями только при новом анализе с их учётом (функция в разработке).',
        ]));
        expect(m.header).toBe('Пара листов: OLD лист 5 (стр. 20) ↔ NEW лист 5 (стр. 10) · регионов: 6');
        expect(m.banners[0].link.href).toBe(`/human-mapping/?object=${OID}&comparison=${PID}&session_id=${SID}&run_id=${RUN}`);
        expect(store.bindingChip()).toBe('Прогон a631b49a · 21.09 · Claude Opus 5 · V3 3.5.2 · текущий ▾');
        expect(store.focusId()).toBe('A-R016');
    });

    it('A (general form) when the focused region has links but no history', async () => {
        const index = {...INDEX, regions: [indexRegion('A-R900', [[20], [10, 11]])]};
        const {m} = await view({index});
        expect(texts(m)).toContain('Анализ нашёл на этих листах смысловые регионы: 1. Связей блоков пока нет — выделите блок слева и справа и соедините их.');
    });

    it('F: no analysis yet — recognition blocks, stamps hidden', async () => {
        const status = statusBody({current_run_id: null, runs: [], latest_attempt: null});
        const {m, store, calls} = await view({status});
        expect(texts(m)).toContain('Смысловые регионы строит анализ изменений (шаг 3). Сейчас можно посмотреть блоки распознавания и проверить, правильно ли сопоставлены листы. Связи блоков сохраняются в конкретный результат анализа и станут доступны после первого анализа.');
        expect(m.hints.find(h => h.key === 'f').actions[0]).toMatchObject({id: 'launch', label: 'К запуску анализа'});
        expect(store.rowChip(ROW_20_10)).toBeNull();
        store.enterBlocks(ROW_20_10);
        await flush();
        expect(calls.filter(u => kind(u) === 'page-blocks'))
            .toEqual([`/api/stage-comparison/sessions/${SID}/pairs/${PID}/block-mapping/source/page-blocks?side=OLD&pages=20`,
                `/api/stage-comparison/sessions/${SID}/pairs/${PID}/block-mapping/source/page-blocks?side=NEW&pages=10`]);
        expect(store.sourceSummary()).toBe('Блоки распознавания: OLD 2 · NEW 1 (штампы скрыты)');
    });

    it('F1: the last attempt failed and there is no earlier result', async () => {
        const status = statusBody({current_run_id: null, runs: [], latest_attempt: {run_id: FAILED, state: 'FAILED',
            reason_code: 'v3_inference_kill_switch', created_at: local(9, 24, 15, 0), completed_at: local(9, 24, 15, 23)}});
        const {m} = await view({status});
        expect(texts(m)).toContain('Последний запуск анализа 24.09 15:23 не завершился: ИИ-анализ отключён администратором.');
        const other = await view({status: {...status, latest_attempt: {...status.latest_attempt, reason_code: 'x_other', completed_at: null}}});
        const f1 = other.m.hints.find(h => h.key === 'f1');
        expect(f1.text).toBe('Последний запуск анализа 24.09 15:00 не завершился: см. подробности.');
        expect(f1.details).toEqual(['x_other']);
        const provider = await view({status: {...status, latest_attempt: {...status.latest_attempt, reason_code: 'v3_provider_unavailable'}}});
        expect(texts(provider.m)).toContain('Последний запуск анализа 24.09 15:23 не завершился: модель недоступна или исчерпан лимит подписки.');
    });

    it('F2: the last attempt failed, the earlier frozen run stays bound', async () => {
        const status = statusBody({latest_attempt: {run_id: FAILED, state: 'FAILED', reason_code: 'x',
            created_at: local(9, 24, 15, 0), completed_at: local(9, 24, 15, 23)}});
        const {m, S} = await view({status});
        expect(S.binding).toEqual({kind: 'LIVE', runId: RUN});
        expect(texts(m)).toContain('Последний запуск анализа 24.09 15:23 не завершился. Показан предыдущий результат a631b49a от 21.09.');
    });

    it('J: analysis running, with and without a bound run', async () => {
        const running = {run_id: NEXT, state: 'RUNNING', reason_code: null, created_at: local(9, 24, 15, 0), completed_at: null};
        let {m} = await view({status: statusBody({latest_attempt: running})});
        expect(texts(m)).toContain('Идёт новый анализ. Решения записываются в прогон a631b49a; новый прогон начнёт с пустой истории связей.');
        ({m} = await view({status: statusBody({latest_attempt: running, current_run_id: null, runs: []})}));
        expect(texts(m)).toContain('Идёт анализ изменений. Смысловые регионы появятся после его завершения.');
        expect(texts(m).some(x => x.startsWith('Смысловые регионы строит'))).toBe(false);
    });

    it('K0 / K-PDF / K-BLOCKS / L / failures 1, 4, 5 by write_block_reason (first reason shown)', async () => {
        const cases = [
            [{source_stale: true, blocks_content_match: true}, 'Файлы распознавания перезаписаны без изменений — работа с блоками доступна.'],
            [{pdf_match: false, write_block_reason: 'SOURCE_PDF_CHANGED', write_block_reasons: ['SOURCE_PDF_CHANGED', 'WRITES_DISABLED']},
                'Документ пары изменился после анализа a631b49a. Решения нельзя будет использовать — перезапустите анализ.'],
            [{source_stale: true, blocks_content_match: false, write_block_reason: 'SOURCE_BLOCKS_CHANGED',
                write_block_reasons: ['SOURCE_BLOCKS_CHANGED', 'WRITES_DISABLED']},
            'Распознавание пары изменилось после анализа a631b49a. Новые решения могли бы указывать на другие блоки, поэтому запись закрыта.'],
            [{}, 'Редактирование смысловых связей в этом разделе ещё не включено. Для правки откройте Human Mapping ↗'],
            [{hm_available: false, hm_reason: 'HM_UI_DATA_MISSING', write_block_reason: 'HM_UNAVAILABLE'},
                'Данные смысловых блоков прогона a631b49a недоступны. Журнал решений показан как есть (идентификаторы блоков текстом).'],
            [{hm_available: false, hm_reason: 'HM_SCOPE_MISMATCH', write_block_reason: 'HM_UNAVAILABLE'},
                'Данные смысловых блоков прогона a631b49a не принадлежат выбранному результату.'],
            [{write_block_reason: 'RUN_INVALID', write_block_reasons: ['RUN_INVALID', 'WRITES_DISABLED']},
                'Результат a631b49a недоступен или изменён вне системы. Решения в его истории не удалены.'],
        ];
        for (const [overrides, text] of cases) {
            const {m} = await view({status: statusBody({runs: [runEntry(overrides)]})});
            expect(texts(m)).toContain(text);
        }
        // The later reason stays in «Подробнее», never replaces the first one.
        const pdf = await view({status: statusBody({runs: [runEntry(cases[1][0])]})});
        expect(pdf.m.banners[0].details).toEqual([cases[3][1]]);
        expect(texts(pdf.m)).not.toContain(cases[3][1]);
    });

    it('K-PDF rasters come from the run assets of Human Mapping (the only case, C9)', async () => {
        const {store} = await view({status: statusBody({runs: [runEntry({pdf_match: null, write_block_reason: 'SOURCE_PDF_CHANGED'})]})});
        expect(store.rasterUrl('OLD', 20))
            .toBe(`/api/human-mapping/objects/${OID}/comparisons/${PID}/assets/old/p020/full_page.png?session_id=${SID}&run_id=${RUN}`);
    });

    it('HM_UNAVAILABLE / RUN_INVALID: no region-index request, history still shown as is', async () => {
        const env = setup({status: statusBody({runs: [runEntry({hm_available: false, hm_reason: 'HM_UI_DATA_UNREADABLE',
            write_block_reason: 'HM_UNAVAILABLE'})]}), reviews: DECIDED.reviews, edits: DECIDED.edits});
        await env.store.load({sessionId: SID, pairId: PID});
        expect(env.calls.map(kind).sort()).toEqual(['block-links', 'reviews', 'status']);
        expect(env.store.model()).toBeNull();
        expect(env.store.journal('')).toHaveLength(6);
        const invalid = setup({status: statusBody({runs: [runEntry({write_block_reason: 'RUN_INVALID'})]})});
        await invalid.store.load({sessionId: SID, pairId: PID});
        expect(invalid.calls.map(kind)).toEqual(['status']);
    });

    it('M1: poisoned history — the server decides, the client counts and marks the events', async () => {
        const reviews = [review('p1', 'A-R099', 'HUMAN_CONFIRMED', [G20], [T10], t(20)),
            review('p2', 'A-R016', 'HUMAN_CONFIRMED', ['blk_elsewhere'], [T10], t(21))];
        const status = statusBody({runs: [runEntry({history: {reviews: 2, block_link_events: 0, poisoned: true,
            poison_codes: ['STALE_REGION', 'BLOCK_OUTSIDE_REGION']}, write_block_reason: 'HISTORY_POISONED',
        write_block_reasons: ['HISTORY_POISONED', 'WRITES_DISABLED']})]});
        const {m, store} = await view({status, reviews});
        expect(texts(m)).toContain('История прогона содержит событий, которые проверка для нового анализа отвергнет: 2. История только дополняется — исправить можно только в следующем прогоне.');
        expect(m.banners[0].details.join('\n')).toContain('регион не найден');
        expect(store.journal('A-R016')[0].mark).toBe('вне региона');
    });

    it('M2 + B: an edge confirmed in one region and rejected in another; chip and head counters', async () => {
        const {m, store} = await view(DECIDED);
        expect(m.conflict.text).toBe('Одна и та же связь закреплена в A-R010 и запрещена в A-R014 — снимок для нового анализа не соберётся.');
        expect(m.conflict.actions[0].label).toBe('Показать конфликт');
        expect(store.rowChip(ROW_20_10).label).toBe('◇6 ✓2 ⊘1 ⚠1');
        expect(store.rowChip(ROW_20_10).title)
            .toBe('Смысловые регионы анализа на этих листах: 6. Закреплено связей: 2, запрещено: 1, требуют внимания: 1.');
        expect(store.summary().text).toBe('Смысловые связи · прогон a631b49a: закреплено 2 · запрещено 1 · требуют внимания 1');
        expect(store.currentDecision('A-R016')).toBe(`Текущее решение: ✓ Подтверждено ${new Intl.DateTimeFormat('ru-RU',
            {day: '2-digit', month: '2-digit'}).format(new Date(t(39)))} ${new Date(t(39)).toTimeString().slice(0, 5)} · закреплено 1. Новое решение заменит его целиком; прежнее останется в журнале.`);
        expect(texts(m)).toContain('В истории прогона a631b49a записей: 6 (решений 3, событий связей 3). Новое решение региона заменит текущее; прежние останутся в журнале.');
        // HUMAN_REJECTED is drawn on the edge only: block frames carry no rejection.
        const conflictEdge = store.model().edges.get(G20 + '|' + T9);
        expect(conflictEdge.state).toBe('CROSS_REGION_CONFLICT');
        expect(store.edgeView(conflictEdge).glyph).toBe('⚠');
        store.showConflict();
        expect(store.state.lens).toMatchObject({mode: 'region', focus: 'A-R010'});
        expect(store.state.selection.link).toBe(G20 + '|' + T9);
    });

    it('B: link created after the region decision (◌)', async () => {
        const edits = [add('e1', 'A-R016', 'human:1', G20, T10, t(30)), add('e9', 'A-R016', 'human:9', G20, 'A-R016:NEW:5', t(50))];
        const reviews = [review('r1', 'A-R016', 'HUMAN_CONFIRMED', [G20], [T10, 'A-R016:NEW:5'], t(40))];
        const {m, store} = await view({reviews, edits});
        expect(texts(m)).toContain('Связей, созданных после решения региона и ещё не закреплённых: 1. Повторите решение, чтобы включить их.');
        expect(store.edgeView(store.model().edges.get(G20 + '|A-R016:NEW:5')).glyph).toBe('◌');
    });

    it('N: a newer current run appears — banner, no silent switch', async () => {
        const env = setup();
        await env.store.load({sessionId: SID, pairId: PID});
        env.state.status = statusBody({current_run_id: NEXT, runs: [runEntry({run_id: NEXT}), runEntry({is_current: false})]});
        await env.store.refreshStatus();
        const m = env.store.messages({rows: ROWS, sheetOf});
        const banner = m.banners.find(b => b.key === 'n');
        expect(banner.text).toBe('Появился новый результат анализа 7f3c21aa (текущий). Решения по блокам относятся к a631b49a и в новый результат не переносятся.');
        expect(banner.actions.map(a => a.label)).toEqual(['Перейти к новому', 'Остаться']);
        expect(env.S.binding.runId).toBe(RUN);
        env.store.dismissNewRun();
        expect(env.store.messages({rows: ROWS, sheetOf}).banners.find(b => b.key === 'n')).toBeUndefined();
    });

    it('O, P and I: row context', async () => {
        let {m} = await view({row: {...ROW_20_10, key: 'suggestion-20', explicitLinkIndex: null}});
        expect(texts(m)).toContain('Пара листов не подтверждена — блоки показаны по предложению.');
        ({m} = await view({row: ROW_21}));
        expect(texts(m)).toContain('У листа OLD лист 6 (стр. 21) нет пары. Показана одна сторона; чтобы соединить блоки, включите «Весь регион».');
        ({m} = await view({row: null, mapBuilt: false}));
        expect(texts(m)).toContain('Пары листов не построены — смысловые блоки открываются по регионам. Постройте карту листов, чтобы видеть регионы по парам листов.');
        ({m} = await view({row: null, mapBuilt: false, status: statusBody({current_run_id: null, runs: [], latest_attempt: null})}));
        expect(texts(m)).toContain('Пары листов не построены. Нажмите «Обработать», чтобы построить карту листов.');
    });

    it('C / D / E headers carry the cardinality of the sheet pair', async () => {
        const {m} = await view({row: {key: 'x', leftPages: [20], rightPages: [10, 26], explicitLinkIndex: 0}});
        expect(m.header).toBe('Пара листов: OLD лист 5 (стр. 20) ↔ NEW стр. 10, 26 · 1→2 · регионов: 6');
        const many = await view({row: {key: 'e', leftPages: [22, 23, 24, 25, 26, 27, 29], rightPages: [31, 32], explicitLinkIndex: 0}});
        expect(many.m.header).toContain(' · 7→2 · регионов: ');
        expect(texts(many.m)).toContain('Показаны видимые страницы; прокрутите для остальных.');
    });

    it('I (API read failure): «Не удалось загрузить смысловые блоки.» with retry, sheets unaffected', async () => {
        const env = setup({fail: {'/block-mapping/status': 'reject'}});
        await env.store.load({sessionId: SID, pairId: PID});
        const m = env.store.messages({rows: ROWS, sheetOf});
        expect(m.banners).toEqual([expect.objectContaining({text: 'Не удалось загрузить смысловые блоки.',
            actions: [{id: 'retry', label: 'Повторить'}]})]);
        expect(env.store.rowChip(ROW_20_10)).toBeNull();
        const index = setup({fail: {'/region-index': {code: 500, body: {detail: 'boom'}}}});
        await index.store.load({sessionId: SID, pairId: PID});
        expect(texts(index.store.messages({rows: ROWS, sheetOf}))).toContain('Не удалось загрузить смысловые блоки.');
    });

    it('bridge check is a GET by the button only, with the texts of §4.3', async () => {
        const {store, calls} = await view(DECIDED);
        expect(count(calls, 'bridge-check')).toBe(0);
        await store.bridgeCheck();
        expect(count(calls, 'bridge-check')).toBe(1);
        expect(store.state.bridge.text).toBe('Снимок для нового анализа собирается: якорей 2 · запретов 1. Это предварительная проверка; якорный анализ пока недоступен.');
        expect(store.state.bridge.details).toBe('f'.repeat(64));
    });

    it('bridge refusal: the specific cause is error.reason, error.code is only its class', async () => {
        const refusal = (code, reason) => ({ok: false, cached: false, checked_at: local(9, 24, 14, 52), result: null,
            error: {code, reason, details: {}}});
        const conflict = await view({...DECIDED, bridge: refusal('BRIDGE_CONFLICT_REVIEW_REQUIRED', 'CONFIRMED_AND_REJECTED_EXACT_EDGE')});
        await conflict.store.bridgeCheck();
        expect(conflict.store.state.bridge.text).toBe('Не собирается: одна и та же связь закреплена в одном регионе и запрещена в другом.');
        expect(conflict.store.state.bridge.conflict).toBe(true);
        const other = await view({...DECIDED, bridge: refusal('BRIDGE_MAPPING_REJECTED', 'INVALID_EVENT_TIMESTAMP')});
        await other.store.bridgeCheck();
        expect(other.store.state.bridge.text).toBe('Не собирается: INVALID_EVENT_TIMESTAMP. Подробности — в журнале.');
        expect(other.store.state.bridge.conflict).toBe(false);
    });
});

describe('regions around the sheet map (C3, П-21) on DEV5-shaped data', () => {
    it('row chips, untouched (3) and unpaired (11) regions', async () => {
        const {store} = setup();
        await store.load({sessionId: SID, pairId: PID});
        expect(store.rowChip(ROW_20_10).label).toBe('◇6');
        expect(store.rowChip(ROWS[0]).label).toBe('◇2');
        expect(store.untouched(ROWS).map(r => r.id)).toEqual(['A-R003', 'A-R005', 'A-R015']);
        expect(store.unpaired(ROWS).map(r => r.id)).toEqual(['A-R003', 'A-R004', 'A-R005', 'A-R006', 'A-R007', 'A-R008',
            'A-R009', 'A-R012', 'A-R013', 'A-R014', 'A-R015']);
        expect(store.segmentLabel(ROW_20_10)).toBe('Смысловые блоки ◇6');
        store.openRow(ROW_20_10);
        const shelves = store.shelves();
        expect(shelves.both.map(r => r.id)).toEqual(['A-R016']);
        expect(shelves.old.map(r => r.id)).toEqual(['A-R010', 'A-R012', 'A-R014']);
        expect(shelves.new.map(r => r.id)).toEqual(['A-R006', 'A-R011']);
        expect(store.state.lens.key).toBe('L20|R10');
        store.setLens('region');
        expect(store.lensPages().OLD.map(p => [p.page, p.inPair])).toEqual([[20, true], [51, false], [52, false]]);
        expect(store.lensPages().NEW.map(p => p.page)).toEqual([5, 10, 26]);
    });

    it('a region opened from the pinned row keeps its lens when the blocks mode starts', async () => {
        const {store} = setup();
        await store.load({sessionId: SID, pairId: PID});
        store.openRegion('A-R003');
        store.enterBlocks(ROW_20_10);
        expect(store.state.lens).toMatchObject({mode: 'region', focus: 'A-R003', key: 'L20|R10', pinned: false});
        expect(store.lensPages().OLD.map(p => p.page)).toEqual([9, 15]);
        store.enterBlocks(ROWS[0]);                       // later a plain switch follows the current row again
        expect(store.state.lens).toMatchObject({mode: 'pair', key: 'L5|R4'});
    });

    it('the rows change keeps the lens on the same composition, else moves and says so', async () => {
        const {store} = setup();
        await store.load({sessionId: SID, pairId: PID});
        store.openRow({key: 'explicit-9', leftPages: [12], rightPages: [18, 19], explicitLinkIndex: 0});
        store.syncRows([{key: 'explicit-0', leftPages: [12], rightPages: [18], explicitLinkIndex: 0}]);
        expect(store.state.lens.key).toBe('L12|R18');
        expect(store.state.rowNotice.text).toBe('Пара листов изменена: было OLD 12 ↔ NEW 18, 19 · стало OLD 12 ↔ NEW 18');
    });

    it('page-map change notice after decisions (MASTER §12, failure 6) with a session mark', async () => {
        const marks = {};
        const storage = {setItem: (k, v) => { marks[k] = v; }, getItem: k => marks[k] || null, removeItem: k => { delete marks[k]; }};
        const {store} = setup({...DECIDED, storage});
        await store.load({sessionId: SID, pairId: PID});
        store.noteSheetLinks(PID, LINKS);                   // baseline, no notice
        expect(store.state.notice).toBeNull();
        store.noteSheetLinks(PID, LINKS.map(l => ({...l})));  // same composition: nothing
        expect(store.state.notice).toBeNull();
        const without = LINKS.filter(l => l.left_pages[0] !== 20);
        store.noteSheetLinks(PID, without);
        expect(store.state.notice.text).toBe('Связь листов сохранена. 3 решения по смысловым блокам остались в силе (прогон a631b49a); теперь они показаны: 3 — вне карты листов.');
        expect(marks[`sbm:ctx:${RUN}:human:1`]).toBe('1');
        expect(store.contextMarked('human:1')).toBe(true);
        store.noteSheetLinks(PID, [...without, {left_pages: [20], right_pages: [9]}]);
        expect(store.state.notice.text).toBe('Связь листов сохранена. 2 решения по смысловым блокам остались в силе (прогон a631b49a); теперь они показаны: 2 — в паре 20 ↔ 9.');
        store.noteSheetLinks(PID, [...without, {left_pages: [20], right_pages: [9]}, {left_pages: [21], right_pages: [10]}]);
        expect(store.state.notice.text).toBe('Связь листов сохранена. 1 решение по смысловым блокам осталось в силе (прогон a631b49a); теперь они показаны: 1 — концы в разных парах листов.');
        // Opening the link in the inspector clears the mark; decisions are never deleted or marked stale.
        store.selectLink(G20 + '|' + T10);
        expect(store.contextMarked('human:1')).toBe(false);
        expect(store.model().decided).toHaveLength(3);
    });

    it('works without session storage (mark just not shown)', async () => {
        const broken = {setItem: () => { throw new Error('denied'); }, getItem: () => { throw new Error('denied'); }, removeItem: () => {}};
        const {store} = setup({...DECIDED, storage: broken});
        await store.load({sessionId: SID, pairId: PID});
        store.noteSheetLinks(PID, LINKS);
        expect(() => store.noteSheetLinks(PID, LINKS.slice(0, 6))).not.toThrow();
        expect(store.state.notice.text).toMatch(/^Связь листов сохранена\./);
    });
});

describe('drawing helpers', () => {
    const {SBM} = setup();
    const rects = {OLD: {left: 0, top: 0, width: 100, height: 100}, NEW: {left: 130, top: 0, width: 100, height: 100}};
    const edge = {key: 'o|n', old_block_id: 'o', new_block_id: 'n', oldPage: 20, newPage: 26};
    const centers = (o, n) => ({OLD: new Map(o ? [['o', o]] : []), NEW: new Map(n ? [['n', n]] : [])});

    it('draws a straight segment between block centres (HM pathPair)', () => {
        const [line] = SBM.computeLinkGeometry({edges: [edge], centers: centers({x: 50, y: 50}, {x: 180, y: 50}), rects});
        expect(line.d).toBe('M 50 50 L 180 50');
        expect(line.ports).toEqual([]);
        expect(line.mid).toEqual({x: 115, y: 50});
    });

    it('sends an end outside the lens to a port on the inner edge of the other panel', () => {
        const [line] = SBM.computeLinkGeometry({edges: [edge], centers: centers({x: 50, y: 40}, null), rects});
        expect(line.d).toBe('M 50 40 L 130 40');
        expect(line.ports).toEqual([{side: 'NEW', x: 130, y: 40, page: 26, kind: 'offlens', single: false}]);
        const [back] = SBM.computeLinkGeometry({edges: [edge], centers: centers(null, {x: 180, y: 60}), rects});
        expect(back.ports[0]).toMatchObject({side: 'OLD', x: 100, y: 60, page: 20, kind: 'offlens'});
    });

    it('clips an end scrolled out of its panel to the panel rectangle', () => {
        const [line] = SBM.computeLinkGeometry({edges: [edge], centers: centers({x: 50, y: 50}, {x: 180, y: 300}), rects});
        expect(line.end.y).toBeLessThanOrEqual(100);
        expect(line.end.x).toBeGreaterThanOrEqual(130);
        expect(line.ports).toEqual([expect.objectContaining({side: 'NEW', kind: 'scrolled'})]);
    });

    it('one panel (state P): ports on its own edge; both ends outside the lens → no line', () => {
        const [line] = SBM.computeLinkGeometry({edges: [edge], centers: centers({x: 50, y: 50}, null), rects: {OLD: rects.OLD, NEW: null}});
        expect(line.ports[0]).toMatchObject({side: 'NEW', x: 100, y: 50, single: true});
        expect(SBM.computeLinkGeometry({edges: [edge], centers: centers(null, null), rects})).toEqual([]);
    });

    it('hides stamps unless shown, except a stamp that is a link end or in a review selection (C26)', () => {
        const blocks = ['t', 's-end', 's-review', 's-plain'].map(id => ({block_id: id, modality: 'TEXT',
            source_block_type: id === 't' ? 'text' : 'stamp'}));
        const opts = {showStamps: false, linkEnds: new Set(['OLD|s-end']), reviewSel: new Set(['OLD|s-review'])};
        expect(SBM.visibleBlocks('OLD', blocks, opts).map(b => b.block_id)).toEqual(['t', 's-end', 's-review']);
        expect(SBM.visibleBlocks('NEW', blocks, opts).map(b => b.block_id)).toEqual(['t']);
        expect(SBM.visibleBlocks('OLD', blocks, {...opts, showStamps: true})).toHaveLength(4);
    });

    it('reads the three HM error body forms (C10)', () => {
        expect(SBM.errorCode({detail: {error: 'REGION_NOT_FOUND', ok: false}})).toBe('REGION_NOT_FOUND');
        expect(SBM.errorCode({detail: 'Human Mapping result unavailable'})).toBe('Human Mapping result unavailable');
        expect(SBM.errorCode({error: 'BLOCK_LINK_ALREADY_EXISTS'})).toBe('BLOCK_LINK_ALREADY_EXISTS');
        expect(SBM.errorCode(null)).toBe('');
    });

    it('chooses the layout by width (UNIFIED §13)', () => {
        expect([1440, 1280, 1279, 900, 899, 600, 599].map(SBM.layoutFor))
            .toEqual(['full', 'full', 'compact', 'compact', 'single', 'single', 'list']);
    });

    it('parses tables like the HM page (array / Markdown / fallback)', () => {
        expect(SBM.parseTable([['a', 'b']])).toEqual([['a', 'b']]);
        expect(SBM.parseTable('| a | b |\n|---|---|\n| 1 | 2 |')).toEqual([['a', 'b'], ['1', '2']]);
        expect(SBM.parseTable('просто текст')).toBeNull();
    });
});
