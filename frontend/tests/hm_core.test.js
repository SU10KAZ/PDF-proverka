import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { createRequire } from 'node:module';

// Parity of human-mapping-core.js with the Human Mapping page the server serves.
// The page functions are cut out of HTML_PAGE (backend/app/api/routers/human_mapping.py)
// exactly as the served page contains them; zero network, zero model calls.
const require = createRequire(import.meta.url);
const core = require('../static/js/human-mapping-core.js');
const routerSource = readFileSync(new URL('../../backend/app/api/routers/human_mapping.py', import.meta.url), 'utf8');
const fixture = letter => JSON.parse(readFileSync(
    new URL(`../../backend/app/data/human_mapping_fixtures/UI_DATA_PAIR_${letter}.json`, import.meta.url), 'utf8'));

function servedPage() {
    const start = routerSource.indexOf("HTML_PAGE = r'''") + "HTML_PAGE = r'''".length;
    return routerSource.slice(start, routerSource.indexOf("'''", start));
}
function pageLine(page, prefix) {
    const line = page.split('\n').find(l => l.startsWith(prefix));
    if (!line) throw new Error('HM page no longer has ' + prefix);
    return line;
}
function loadPage() {
    const page = servedPage();
    const script = ['var history=[],linkEvents=[],selected={OLD:[],NEW:[]},showAll=false,selectedLinkId=null;',
        pageLine(page, 'function latest(r)'), pageLine(page, 'function baseProposedLinks(r)'),
        pageLine(page, 'function effectiveLinks(r)'), pageLine(page, 'function visibleLinks(r)')].join('\n');
    const sandbox = {};
    vm.runInNewContext(script, sandbox);
    return sandbox;
}

// Deterministic generator: histories of link events over a region, valid and invalid alike.
function lcg(seed) {
    let x = seed >>> 0;
    return () => (x = (Math.imul(x, 1664525) + 1013904223) >>> 0) / 2 ** 32;
}
function randomEvents(region, rnd, count) {
    const olds = [...region.old_blocks.map(b => b.id), 'ghost-old'];
    const news = [...region.new_blocks.map(b => b.id), 'ghost-new'];
    const pick = list => list[Math.floor(rnd() * list.length)];
    const events = [];
    for (let i = 0; i < count; i++) {
        const known = core.effectiveLinks(region, events);
        const kind = pick(['ADD_BLOCK_LINK', 'ADD_BLOCK_LINK', 'DELETE_BLOCK_LINK', 'REASSIGN_BLOCK_LINK']);
        const target = known.length ? pick(known) : null;
        const e = {event_type: kind, region_id: rnd() < 0.9 ? region.id : 'OTHER', link_id: 'human:' + i,
            old_block_id: pick(olds), new_block_id: pick(news)};
        if (kind === 'DELETE_BLOCK_LINK' && target && rnd() < 0.7) Object.assign(e, {link_id: target.link_id,
            old_block_id: target.old_block_id, new_block_id: target.new_block_id});
        if (kind === 'REASSIGN_BLOCK_LINK') e.previous_link_id = target && rnd() < 0.8 ? target.link_id : 'human:missing';
        events.push(e);
    }
    return events;
}
function syntheticRegions() {
    const blocks = (prefix, n, page) => Array.from({length: n}, (_, i) => ({id: `${prefix}${i}`, page}));
    return [
        {id: 'S-11', old_blocks: blocks('a', 1, 1), new_blocks: blocks('b', 1, 7), pages: {OLD: [], NEW: []}},
        {id: 'S-1N', old_blocks: blocks('c', 1, 1), new_blocks: blocks('d', 3, 7), pages: {OLD: [], NEW: []}},
        {id: 'S-N1', old_blocks: blocks('e', 3, 1), new_blocks: blocks('f', 1, 7), pages: {OLD: [], NEW: []}},
        {id: 'S-NN', old_blocks: blocks('g', 3, 1), new_blocks: blocks('h', 2, 7), pages: {OLD: [], NEW: []}},
        {id: 'S-EMPTY', old_blocks: [], new_blocks: blocks('i', 2, 7), membership_state: {OLD: 'EMPTY', NEW: 'MAPPED'},
            pages: {OLD: [{page: 3, blocks: blocks('p', 2, 3)}], NEW: [{page: 7, blocks: blocks('i', 2, 7)}]}},
    ];
}

describe('human-mapping-core — parity with the served HM page', () => {
    const page = loadPage();
    const regions = [...fixture('A').regions, ...fixture('B').regions, ...syntheticRegions()];

    it('proposes the same AI links for every fixture and synthetic region', () => {
        expect(regions.length).toBeGreaterThan(40);
        for (const region of regions) {
            expect(core.baseProposedLinks(region)).toEqual(page.baseProposedLinks(region));
        }
        expect(core.baseProposedLinks(syntheticRegions()[1]).map(l => l.membership_kind))
            .toEqual(['INHERITED_GROUP_1_N', 'INHERITED_GROUP_1_N', 'INHERITED_GROUP_1_N']);
        expect(core.baseProposedLinks(syntheticRegions()[3])).toEqual([]);
    });

    it('replays ADD/DELETE/REASSIGN histories exactly like the page', () => {
        let compared = 0;
        regions.forEach((region, index) => {
            const rnd = lcg(1000 + index);
            for (let round = 0; round < 6; round++) {
                const events = randomEvents(region, rnd, 4 + round * 3);
                page.linkEvents = events;
                expect(core.effectiveLinks(region, events)).toEqual(page.effectiveLinks(region));
                compared++;
            }
        });
        expect(compared).toBe(regions.length * 6);
    });

    it('takes the last appended review of a region as its verdict', () => {
        const region = syntheticRegions()[0];
        const reviews = [
            {region_id: 'S-11', status: 'HUMAN_CONFIRMED', timestamp: '2026-09-24T10:00:00+00:00'},
            {region_id: 'OTHER', status: 'HUMAN_REJECTED', timestamp: '2026-09-24T11:00:00+00:00'},
            {region_id: 'S-11', status: 'HUMAN_UNCERTAIN', timestamp: '2026-09-24T09:00:00+00:00'},
        ];
        page.history = reviews;
        expect(core.latestReview(region, reviews)).toBe(page.latest(region));
        expect(core.regionStatus(region, reviews)).toBe(page.stat(region));
        expect(core.regionStatus(region, reviews)).toBe('HUMAN_UNCERTAIN');
        page.history = [];
        expect(core.regionStatus(region, [])).toBe(page.stat(region));
    });

    it('uses the same straight dotted path and bbox percentages as the page', () => {
        const pathPair = pageLine(servedPage(), 'function pathPair(');
        expect(pathPair).toContain('const d=`M ${a.x} ${a.y} L ${b.x} ${b.y}`');
        expect(core.linkPath({x: 1, y: 2}, {x: 30, y: 40})).toBe('M 1 2 L 30 40');
        const overlay = pageLine(servedPage(), 'function overlay(');
        expect(overlay).toContain('left:${x*100}%;top:${y*100}%;width:${(w-x)*100}%;height:${(h-y)*100}%');
        const box = core.bboxPercent([0.1, 0.2, 0.5, 0.6]);
        for (const [key, value] of Object.entries({left: 10, top: 20, width: 40, height: 40})) expect(box[key]).toBeCloseTo(value, 9);
    });
});

describe('human-mapping-core — edge states follow the bridge fold', () => {
    const region = {id: 'R', old_blocks: [{id: 'o1'}, {id: 'o2'}], new_blocks: [{id: 'n1'}, {id: 'n2'}], pages: {OLD: [], NEW: []}};
    const add = (id, o, n, timestamp) => ({event_type: 'ADD_BLOCK_LINK', region_id: 'R', link_id: id,
        old_block_id: o, new_block_id: n, timestamp});
    const review = (id, status, old, nw, timestamp) => ({review_id: id, region_id: 'R', status,
        old_block_ids: old, new_block_ids: nw, timestamp});

    it('marks links without a verdict, uncertain, outside the selection, newer than the verdict', () => {
        const edits = [add('h1', 'o1', 'n1', '2026-09-24T10:00:00.000001+00:00'),
            add('h2', 'o2', 'n2', '2026-09-24T10:00:00.000003+00:00')];
        const state = (reviews) => Object.fromEntries(core.deriveEdgeStates(region, reviews, edits).map(s => [s.link_id, s.state]));
        expect(state([])).toEqual({h1: 'UNREVIEWED', h2: 'UNREVIEWED'});
        expect(state([review('r1', 'HUMAN_UNCERTAIN', ['o1'], ['n1'], '2026-09-24T11:00:00+00:00')])).toEqual({h1: 'UNCERTAIN', h2: 'UNCERTAIN'});
        // Review between the two links (microsecond precision, other offset): h2 is newer.
        const between = review('r2', 'HUMAN_CONFIRMED', ['o1', 'o2'], ['n1', 'n2'], '2026-09-24T13:00:00.000002+03:00');
        expect(state([between])).toEqual({h1: 'ANCHOR', h2: 'NOT_COVERED_NEWER'});
        expect(state([review('r3', 'HUMAN_REJECTED', ['o1'], ['n1'], '2026-09-24T12:00:00+00:00')])).toEqual({h1: 'FORBIDDEN', h2: 'OUTSIDE_SELECTION'});
    });

    it('reports an edge anchored in one region and forbidden in another as a conflict', () => {
        const other = {...region, id: 'Q'};
        const edits = [{...add('h1', 'o1', 'n1', '2026-09-24T10:00:00+00:00'), event_id: 'e1'},
            {...add('h9', 'o1', 'n1', '2026-09-24T10:00:01+00:00'), region_id: 'Q', event_id: 'e2'}];
        const reviews = [review('r1', 'HUMAN_CONFIRMED', ['o1'], ['n1'], '2026-09-24T11:00:00+00:00'),
            {...review('r2', 'HUMAN_REJECTED', ['o1'], ['n1'], '2026-09-24T11:00:01+00:00'), region_id: 'Q'}];
        const run = core.runConstraints([region, other], reviews, edits);
        expect(run.anchors.map(a => a.region_id)).toEqual(['R']);
        expect(run.rejected.map(a => a.region_id)).toEqual(['Q']);
        expect(run.conflicts).toEqual([{old_block_id: 'o1', new_block_id: 'n1', anchor_regions: ['R'], forbidden_regions: ['Q']}]);
        expect(core.historyIssues({regions: [region, other], reviews, edits}).map(i => i.code)).toEqual(['CONFIRMED_AND_REJECTED_EXACT_EDGE']);
    });

    it('treats a rejected link as one forbidden edge, never as a blocked block', () => {
        const edits = [add('h1', 'o1', 'n1', '2026-09-24T10:00:00+00:00'), add('h2', 'o1', 'n2', '2026-09-24T10:00:01+00:00')];
        const reviews = [review('r1', 'HUMAN_REJECTED', ['o1'], ['n1'], '2026-09-24T11:00:00+00:00')];
        const c = core.regionConstraints(region, reviews, edits);
        expect(c.rejected.map(r => r.link_id)).toEqual(['h1']);
        expect(core.deriveEdgeStates(region, reviews, edits).find(s => s.link_id === 'h2').state).toBe('OUTSIDE_SELECTION');
        expect(core.preValidateLinkEvent({eventType: 'ADD_BLOCK_LINK', region, events: edits, linkId: 'h3',
            oldBlockId: 'o1', newBlockId: 'n2'})).toEqual({ok: false, error: 'BLOCK_LINK_ALREADY_EXISTS'});
        expect(core.preValidateLinkEvent({eventType: 'ADD_BLOCK_LINK', region, events: edits, linkId: 'h3',
            oldBlockId: 'o2', newBlockId: 'n1'}).ok).toBe(true);
    });
});

describe('human-mapping-core — decisions compile into ordinary HM events', () => {
    const region = {id: 'R', old_blocks: [{id: 'o1'}, {id: 'o2'}], new_blocks: [{id: 'n1'}, {id: 'n2'}], pages: {OLD: [], NEW: []}};
    let counter = 0;
    const newLinkId = () => 'human:t' + (++counter);
    const add = (id, o, n, timestamp) => ({event_type: 'ADD_BLOCK_LINK', region_id: 'R', link_id: id,
        old_block_id: o, new_block_id: n, timestamp});
    const edge = (o, n) => ({old_block_id: o, new_block_id: n});

    it('confirms a new link: ADD then a region review covering exactly the confirmed edges', () => {
        const out = core.compileDecision(region, [], [], {kind: 'CONFIRM_LINK', edge: edge('o1', 'n2')}, {newLinkId});
        expect(out.kind).toBe('EVENTS');
        expect(out.linkEvents.map(e => [e.event_type, e.old_block_id, e.new_block_id])).toEqual([['ADD_BLOCK_LINK', 'o1', 'n2']]);
        expect(out.review).toMatchObject({status: 'HUMAN_CONFIRMED', old_block_ids: ['o1'], new_block_ids: ['n2'], previous_review_id: null});
        expect(out.plan.anchors.map(a => [a.old_block_id, a.new_block_id])).toEqual([['o1', 'n2']]);
        expect(out.plan.zeroConstraint).toBe(false);
    });

    it('lists edges that a cartesian selection would anchor "along the way"', () => {
        const edits = [add('h1', 'o1', 'n1', '2026-09-24T10:00:00+00:00'), add('h2', 'o2', 'n2', '2026-09-24T10:00:01+00:00'),
            add('h3', 'o1', 'n2', '2026-09-24T10:00:02+00:00')];
        const reviews = [{review_id: 'r1', region_id: 'R', status: 'HUMAN_CONFIRMED', old_block_ids: ['o1'], new_block_ids: ['n1'],
            timestamp: '2026-09-24T11:00:00+00:00'}];
        const out = core.compileDecision(region, reviews, edits, {kind: 'CONFIRM_LINK', edge: edge('o2', 'n2')}, {newLinkId});
        expect(out.linkEvents).toEqual([]);
        expect(out.review).toMatchObject({old_block_ids: ['o1', 'o2'], new_block_ids: ['n1', 'n2'], previous_review_id: 'r1'});
        expect(out.plan.extra.map(e => e.link_id)).toEqual(['h3']);
        expect(out.plan.replaces).toMatchObject({review_id: 'r1', status: 'HUMAN_CONFIRMED'});
    });

    it('asks before a rejection would replace confirmed links; "delete only" writes a DELETE', () => {
        const edits = [add('h1', 'o1', 'n1', '2026-09-24T10:00:00+00:00'), add('h2', 'o2', 'n2', '2026-09-24T10:00:01+00:00')];
        const reviews = [{review_id: 'r1', region_id: 'R', status: 'HUMAN_CONFIRMED', old_block_ids: ['o1', 'o2'],
            new_block_ids: ['n1', 'n2'], timestamp: '2026-09-24T11:00:00+00:00'}];
        const ask = core.compileDecision(region, reviews, edits, {kind: 'REJECT_LINK', edge: edge('o2', 'n2')}, {newLinkId});
        expect(ask).toMatchObject({kind: 'CHOICE', choice: 'REJECT_WITH_ANCHORS', options: ['DELETE_ONLY', 'REJECT_REPLACING', 'CANCEL']});
        const del = core.compileDecision(region, reviews, edits, {kind: 'REJECT_LINK', edge: edge('o2', 'n2'), choice: 'DELETE_ONLY'}, {newLinkId});
        expect(del.linkEvents.map(e => [e.event_type, e.link_id])).toEqual([['DELETE_BLOCK_LINK', 'h2']]);
        expect(del.review).toBeNull();
        const replace = core.compileDecision(region, reviews, edits, {kind: 'REJECT_LINK', edge: edge('o2', 'n2'), choice: 'REJECT_REPLACING'}, {newLinkId});
        expect(replace.review).toMatchObject({status: 'HUMAN_REJECTED', old_block_ids: ['o2'], new_block_ids: ['n2']});
        expect(replace.plan.lost.map(e => e.link_id)).toEqual(['h1']);
        expect(replace.plan.flipped.map(e => [e.link_id, e.before, e.after])).toEqual([['h2', 'ANCHOR', 'FORBIDDEN']]);
    });

    it('warns that a region verdict without links constrains nothing (N↔N regions)', () => {
        const out = core.compileDecision(region, [], [], {kind: 'REGION_DECISION', status: 'HUMAN_CONFIRMED'}, {newLinkId});
        expect(out.review).toMatchObject({old_block_ids: ['o1', 'o2'], new_block_ids: ['n1', 'n2']});
        expect(out.plan.zeroConstraint).toBe(true);
    });

    it('refuses selections outside the region and link ids it cannot mint', () => {
        expect(core.compileDecision(region, [], [], {kind: 'REGION_DECISION', status: 'HUMAN_CONFIRMED', oldIds: ['x']}, {newLinkId}))
            .toEqual({kind: 'ERROR', error: 'SELECTION_OUTSIDE_REGION'});
        expect(core.compileDecision(region, [], [], {kind: 'CONFIRM_LINK', edge: edge('o1', 'n1')}))
            .toEqual({kind: 'ERROR', error: 'LINK_ID_FACTORY_REQUIRED'});
        expect(core.compileDecision(region, [], [], {kind: 'CONFIRM_LINK'}, {newLinkId}))
            .toEqual({kind: 'ERROR', error: 'EDGE_REQUIRED'});
        const pairs = Array.from({length: 51}, () => ['o1', 'n1']);
        expect(core.compileDecision(region, [], [], {kind: 'CONNECT', pairs}, {newLinkId}))
            .toEqual({kind: 'ERROR', error: 'TOO_MANY_EDGES'});
    });

    it('reassigns with a new link id and previous_link_id (contract V1.2.4)', () => {
        const edits = [add('h1', 'o1', 'n1', '2026-09-24T10:00:00+00:00')];
        const out = core.compileDecision(region, [], edits, {kind: 'REASSIGN_LINK', linkId: 'h1', side: 'NEW', blockId: 'n2'}, {newLinkId});
        expect(out.linkEvents[0]).toMatchObject({event_type: 'REASSIGN_BLOCK_LINK', old_block_id: 'o1', new_block_id: 'n2',
            previous_link_id: 'h1', previous_old_block_id: 'o1', previous_new_block_id: 'n1'});
        expect(core.compileDecision(region, [], edits, {kind: 'REASSIGN_LINK', linkId: 'h1', side: 'NEW', blockId: 'n1'}, {newLinkId}))
            .toMatchObject({kind: 'EVENTS', noop: true});
    });
});

describe('human-mapping-core — sheet lens', () => {
    const region = (id, oldPages, newPages) => ({id, pages: {OLD: oldPages, NEW: newPages}});
    const row = (l, r) => ({leftPages: l, rightPages: r});

    it('keys a sheet group by page composition', () => {
        expect(core.groupKey([19, 12], [18])).toBe('L12,19|R18');
        expect(core.groupKey([12], [])).toBe('L12|R');
    });

    it('classifies a region against a row without hiding anything', () => {
        expect(core.rel(region('A', [20], [10]), row([20], [10]))).toBe('INSIDE');
        expect(core.rel(region('A', [20, 51], [10, 26]), row([20], [10]))).toBe('BOTH_PARTIAL');
        expect(core.rel(region('A', [20], [5]), row([20], [10]))).toBe('OLD_ONLY');
        expect(core.rel(region('A', [3], [10]), row([20], [10]))).toBe('NEW_ONLY');
        expect(core.rel(region('A', [3], [5]), row([20], [10]))).toBe('NONE');
        expect(core.rel({id: 'B', pages: {OLD: [{page: 20, blocks: []}], NEW: [{page: 10, blocks: []}]}}, row([20], [10]))).toBe('INSIDE');
    });

    it('separates regions outside every matched pair from regions without a shared pair', () => {
        const rows = [row([20], [10]), row([21], [11]), row([30], [])];
        const regions = [region('both', [20], [10]), region('split', [20], [11]), region('old-only-map', [30], [40]), region('none', [50], [60])];
        expect(core.untouchedRegions(regions, rows).map(r => r.id)).toEqual(['old-only-map', 'none']);
        expect(core.unpairedRegions(regions, rows).map(r => r.id)).toEqual(['split', 'old-only-map', 'none']);
    });

    it('places decided links relative to saved sheet links only', () => {
        const saved = [{left_pages: [12], right_pages: [18, 19]}, {left_pages: [13], right_pages: [20]}];
        expect(core.placementClass(12, 19, saved)).toEqual({kind: 'IN_ROW', rows: ['L12|R18,19']});
        expect(core.placementClass(12, 20, saved)).toEqual({kind: 'CROSS_ROWS', rows: []});
        expect(core.placementClass(12, 99, saved)).toEqual({kind: 'OFF_MAP', rows: []});
        const after = [{left_pages: [12], right_pages: [18]}, {left_pages: [13], right_pages: [20]}];
        expect(core.placementChanged(core.placementClass(12, 19, saved), core.placementClass(12, 19, after))).toBe(true);
        expect(core.placementChanged(core.placementClass(12, 18, saved), core.placementClass(12, 18, [{left_pages: [12], right_pages: [19, 18]}]))).toBe(false);
    });

    it('keeps pan/zoom per side: lock stops panning only, zoom stays within 0.6–1.8', () => {
        let v = core.createView();
        v = core.panView(v, 10, 5);
        expect(v).toMatchObject({x: 10, y: 5});
        v = core.lockView(v, true);
        expect(core.panView(v, 100, 100)).toBe(v);
        expect(core.zoomView(v, 5).z).toBe(1.8);
        expect(core.zoomView(v, 0.1).z).toBe(0.6);
        expect(core.resetView(v)).toMatchObject({x: 0, y: 0, z: 1, locked: true});
    });

    it('clips a link to its panel so the end outside becomes a port', () => {
        const clipped = core.clipSegment({x: 10, y: 10}, {x: 210, y: 10}, {left: 0, top: 0, width: 100, height: 100});
        expect(clipped).toEqual({a: {x: 10, y: 10}, b: {x: 100, y: 10}, clippedA: false, clippedB: true});
        expect(core.clipSegment({x: 150, y: 10}, {x: 210, y: 10}, {left: 0, top: 0, width: 100, height: 100})).toBeNull();
    });
});
