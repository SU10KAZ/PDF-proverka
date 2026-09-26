// Pre-analysis prelinks (prelink-core.js): pure rules — star edges, the N↔N node, and the promotion compiler
// that must give exactly the events of HumanMappingCore's per-edge intents. No DOM, no network, no model.
import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';

const require = createRequire(import.meta.url);
const Core = require('../static/js/human-mapping-core.js');
const PL = require('../static/js/prelink-core.js');
const read = path => readFileSync(new URL(path, import.meta.url), 'utf8');

const item = (olds, news, id = 'pl_x') => ({prelink_id: id, label: 'PL-1', old_blocks: olds.map(b => ({block_id: b})),
    new_blocks: news.map(b => ({block_id: b}))});

// Region R: OLD o1, o2 ↔ NEW n1, n2 (N:N — no AI links), all mapped.
const REGION = {id: 'R', old_blocks: [{id: 'o1', page: 1}, {id: 'o2', page: 1}], new_blocks: [{id: 'n1', page: 1}, {id: 'n2', page: 1}],
    membership_state: {OLD: 'MAPPED', NEW: 'MAPPED'}, pages: {OLD: [], NEW: []}};
const at = s => `2026-09-26T10:00:${String(s).padStart(2, '0')}+00:00`;
const add = (o, n, s, id = `human:${o}${n}`) => ({event_type: 'ADD_BLOCK_LINK', region_id: 'R', link_id: id, old_block_id: o,
    new_block_id: n, timestamp: at(s)});
const review = (status, olds, news, s, id = 'rv' + s) => ({review_id: id, region_id: 'R', status, old_block_ids: olds,
    new_block_ids: news, timestamp: at(s)});
const edge = (o, n) => ({old_block_id: o, new_block_id: n});
const ids = e => `human:${e.old_block_id}${e.new_block_id}`;

describe('shape of a prelink', () => {
    it('1→1 and 1→N / N→1 are the drawn star; N↔N has no pairs', () => {
        expect(PL.edgesOf(item(['o1'], ['n1']))).toEqual([edge('o1', 'n1')]);
        expect(PL.edgesOf(item(['o1'], ['n1', 'n2']))).toEqual([edge('o1', 'n1'), edge('o1', 'n2')]);
        expect(PL.edgesOf(item(['o1', 'o2'], ['n1']))).toEqual([edge('o1', 'n1'), edge('o2', 'n1')]);
        expect(PL.edgesOf(item(['o1', 'o2'], ['n1', 'n2']))).toEqual([]);
        expect(['1:1', '1:N', 'N:1', 'N:N'].map(PL.cardinalityLabel)).toEqual(['1→1', '1→N', 'N→1', 'N↔N']);
        expect([PL.cardinality(1, 1), PL.cardinality(1, 3), PL.cardinality(2, 1), PL.cardinality(2, 2)]).toEqual(['1:1', '1:N', 'N:1', 'N:N']);
    });

    it('draws a star as segments and N↔N as spokes to one node in the gap, never a grid', () => {
        const centers = {OLD: new Map([['o1', {x: 100, y: 100}], ['o2', {x: 100, y: 300}]]),
            NEW: new Map([['n1', {x: 700, y: 120}], ['n2', {x: 700, y: 280}]])};
        const rects = {OLD: {left: 0, width: 400}, NEW: {left: 600, width: 400}};
        const [star] = PL.composeLines([item(['o1'], ['n1', 'n2'])], centers, rects);
        expect(star.segments).toHaveLength(2);
        expect(star.node).toBeNull();
        const [group] = PL.composeLines([item(['o1', 'o2'], ['n1', 'n2'])], centers, rects);
        expect(group.node).toEqual({x: 500, y: 200});
        expect(group.segments).toHaveLength(4);   // one spoke per block, not 2 × 2 edges
        expect(group.segments.every(s => s.d.endsWith('L 500 200'))).toBe(true);
        // An end scrolled below its panel is pinned to the panel's border, never drawn over the page.
        const [down] = PL.composeLines([item(['o1'], ['n1'])], {OLD: centers.OLD, NEW: new Map([['n1', {x: 700, y: 900}]])},
            {OLD: {left: 0, top: 0, width: 400, height: 500}, NEW: {left: 600, top: 0, width: 400, height: 500}});
        expect(down.segments[0].d).toBe('M 100 100 L 700 494');
        const [across] = PL.composeLines([item(['o1'], ['n1'])], {OLD: centers.OLD, NEW: new Map([['n1', {x: 900, y: 700}]])},
            {OLD: {left: 0, top: 0, width: 400, height: 500}, NEW: {left: 600, top: 0, width: 400, height: 500}});
        expect(across.segments[0].d).toBe('M 100 100 L 633.3333333333334 500');   // along the line, on the bottom border
        // The N↔N node stays inside the panels even when the ends are scrolled far below.
        const far = {OLD: new Map([['o1', {x: 100, y: 1900}], ['o2', {x: 100, y: 2100}]]),
            NEW: new Map([['n1', {x: 700, y: 1900}], ['n2', {x: 700, y: 2100}]])};
        const [low] = PL.composeLines([item(['o1', 'o2'], ['n1', 'n2'])], far,
            {OLD: {left: 0, top: 0, width: 400, height: 500}, NEW: {left: 600, top: 0, width: 400, height: 500}});
        expect(low.node).toEqual({x: 500, y: 494});
        expect(low.segments.every(seg => seg.d.split(' ').map(Number).filter(Number.isFinite).every(v => v <= 1000))).toBe(true);
        // Ends not rendered in the lens are simply not drawn.
        expect(PL.composeLines([item(['o9'], ['n1'])], centers, rects)).toEqual([]);
    });
});

describe('promotion compiles like the per-edge intents of HumanMappingCore', () => {
    const cases = [
        ['CONFIRM_LINK', 'HUMAN_CONFIRMED', [], []],
        ['CONFIRM_LINK', 'HUMAN_CONFIRMED', [review('HUMAN_CONFIRMED', ['o2'], ['n2'], 5)], [add('o2', 'n2', 1)]],
        ['CONFIRM_LINK', 'HUMAN_CONFIRMED', [review('HUMAN_REJECTED', ['o2'], ['n2'], 5)], [add('o2', 'n2', 1)]],
        ['REJECT_LINK', 'HUMAN_REJECTED', [], []],
        ['REJECT_LINK', 'HUMAN_REJECTED', [review('HUMAN_REJECTED', ['o2'], ['n2'], 5)], [add('o2', 'n2', 1)]],
        ['UNCERTAIN_LINK', 'HUMAN_UNCERTAIN', [], []],
        ['UNCERTAIN_LINK', 'HUMAN_UNCERTAIN', [review('HUMAN_UNCERTAIN', ['o2'], ['n2'], 5)], []],
        ['UNCERTAIN_LINK', 'HUMAN_UNCERTAIN', [review('HUMAN_CONFIRMED', ['o1', 'o2'], ['n1', 'n2'], 5)],
            [add('o1', 'n1', 1), add('o2', 'n2', 2)]],
    ];
    it.each(cases)('%s over %j (one edge) gives the same events as the core', (kind, status, reviews, edits) => {
        const e = edge('o1', 'n1');
        const core = Core.compileDecision(REGION, reviews, edits, {kind, edge: e}, {newLinkId: () => ids(e)});
        const mine = PL.compilePromotion(Core, REGION, reviews, edits, {edges: [e], status, linkIdOf: ids});
        expect(mine.kind).toBe(core.kind);
        expect(mine.linkEvents).toEqual(core.linkEvents);
        expect(mine.review).toEqual(core.review);
        expect(mine.plan.anchors).toEqual(core.plan.anchors);
        expect(mine.plan.forbidden).toEqual(core.plan.forbidden);
        expect(mine.plan.lost).toEqual(core.plan.lost);
    });

    it('a star is one queue: missing edges added with the server ids, one review over all of them', () => {
        const out = PL.compilePromotion(Core, REGION, [], [add('o1', 'n2', 1)], {edges: [edge('o1', 'n1'), edge('o1', 'n2')],
            status: 'HUMAN_CONFIRMED', linkIdOf: ids});
        expect(out.linkEvents.map(x => x.link_id)).toEqual(['human:o1n1']);   // o1→n2 already effective
        expect(out.review).toMatchObject({status: 'HUMAN_CONFIRMED', old_block_ids: ['o1'], new_block_ids: ['n1', 'n2']});
        expect(out.plan.anchors.map(a => a.new_block_id).sort()).toEqual(['n1', 'n2']);
    });

    it('rejecting over other anchors needs the choice of the core, then replaces the verdict', () => {
        const reviews = [review('HUMAN_CONFIRMED', ['o2'], ['n2'], 5)], edits = [add('o2', 'n2', 1)];
        const first = PL.compilePromotion(Core, REGION, reviews, edits, {edges: [edge('o1', 'n1')], status: 'HUMAN_REJECTED', linkIdOf: ids});
        expect(first).toMatchObject({kind: 'CHOICE', choice: 'REJECT_WITH_ANCHORS'});
        const core = Core.compileDecision(REGION, reviews, edits, {kind: 'REJECT_LINK', edge: edge('o1', 'n1'), choice: 'REJECT_REPLACING'},
            {newLinkId: () => 'human:o1n1'});
        const mine = PL.compilePromotion(Core, REGION, reviews, edits, {edges: [edge('o1', 'n1')], status: 'HUMAN_REJECTED',
            linkIdOf: ids, choice: 'REJECT_REPLACING'});
        expect(mine.review).toEqual(core.review);
        expect(mine.plan.lost.map(x => x.new_block_id)).toEqual(['n2']);
    });

    it('never writes outside the region and never without edges', () => {
        expect(PL.compilePromotion(Core, REGION, [], [], {edges: [edge('o9', 'n1')], status: 'HUMAN_CONFIRMED', linkIdOf: ids}))
            .toEqual({kind: 'ERROR', error: 'BLOCK_NOT_IN_REGION'});
        expect(PL.compilePromotion(Core, REGION, [], [], {edges: [], status: 'HUMAN_CONFIRMED', linkIdOf: ids}).kind).toBe('ERROR');
    });
});

describe('texts and wiring', () => {
    it('the launch line says the AI does not see the links, and is empty when the feature is off', () => {
        const view = {capabilities: {drafts_api: true}, prelinks: [{}, {}], launch_summary: {stale_not_reconciled: 1}};
        expect(PL.launchLine(view)).toBe('Предварительные связи: 2. ИИ их не видит — после анализа мы сравним их с результатом. Устарели: 1 — сверяться не будут.');
        expect(PL.launchLine({...view, capabilities: {drafts_api: false}})).toBe('');
        expect(PL.launchLine({...view, prelinks: []})).toBe('');
        expect(PL.TEXT.D1).toContain('ИИ их не видит');
    });

    it('only the drafts client writes /prelinks; the workspace module keeps its two HM POSTs', () => {
        const client = read('../static/js/prelink-drafts-client.js');
        const module = read('../static/js/stage-block-mapping.js');
        const core = read('../static/js/prelink-core.js');
        expect(client).toContain("json('POST'");
        expect(client).toContain("json('PUT'");
        expect(client).toContain("{method: 'DELETE'}");
        expect(module).not.toContain('/prelinks');
        expect(core).not.toMatch(/fetch\(|XMLHttpRequest/);
        // Nothing of a prelink is ever addressed to a model-facing endpoint.
        for (const source of [client, core]) expect(source).not.toMatch(/production\/run|\/ai-|provider|claude/i);
    });

    it('the page loads the prelink modules after the core and before the workspace, with the capability token', () => {
        const html = read('../index.html');
        const main = read('../../backend/app/main.py');
        const order = ['human-mapping-core.js', '<script type="application/json" id="stage-prelink-caps">{{prelink_caps}}</script>', 'prelink-core.js',
            'prelink-drafts-client.js', 'stage-block-mapping.js'].map(x => html.indexOf(x));
        expect(order.every(i => i > 0)).toBe(true);
        expect([...order].sort((a, b) => a - b)).toEqual(order);
        expect(main).toContain('html = html.replace("{{prelink_caps}}", json.dumps({"drafts_api": drafts_enabled()}))');
        const tuple = main.slice(main.indexOf('pc_js = ['), main.indexOf('js_mtimes = ['));
        for (const name of ["'prelink-core.js'", "'prelink-drafts-client.js'"]) expect(tuple).toContain(name);
    });
});
