(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.HumanMappingCore = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';
    // Pure Human Mapping rules shared by the stage-2 block workspace. No DOM, no fetch.
    // Link replay mirrors the served HM page (human_mapping.py baseProposedLinks/effectiveLinks)
    // and validation.py; edge states and constraints mirror human_mapping_bridge._build.
    // Parity with both is pinned by frontend/tests/hm_core.test.js and
    // backend/tests/stage_block_mapping/test_hm_core_parity.py.
    const SIDES = ['OLD', 'NEW'];
    const REVIEW_STATUSES = ['HUMAN_CONFIRMED', 'HUMAN_REJECTED', 'HUMAN_UNCERTAIN'];
    const LINK_EVENT_TYPES = ['ADD_BLOCK_LINK', 'DELETE_BLOCK_LINK', 'REASSIGN_BLOCK_LINK'];
    const EDGE = Object.freeze({
        UNREVIEWED: 'UNREVIEWED', UNCERTAIN: 'UNCERTAIN', OUTSIDE_SELECTION: 'OUTSIDE_SELECTION',
        NOT_COVERED_NEWER: 'NOT_COVERED_NEWER', ANCHOR: 'ANCHOR', FORBIDDEN: 'FORBIDDEN',
        INVALID_TIMESTAMP: 'INVALID_TIMESTAMP',
    });
    const CONSTRAINED = [EDGE.ANCHOR, EDGE.FORBIDDEN];
    const ZOOM_MIN = 0.6;
    const ZOOM_MAX = 1.8;
    const MAX_EDGES_PER_ACTION = 50;

    function ids(blocks) { return (blocks || []).map(b => b.id); }
    function edgeKey(oldId, newId) { return oldId + '|' + newId; }
    function uniq(values) { return [...new Set(values)]; }
    function forRegion(events, region) { return (events || []).filter(e => e.region_id === region.id); }

    // ── Link replay (byte-for-byte semantics of the served HM page) ─────────
    function baseProposedLinks(region) {
        const olds = ids(region.old_blocks), news = ids(region.new_blocks);
        if (olds.length === 1 && news.length === 1) return [{link_id: `ai:${region.id}:${olds[0]}:${news[0]}`,
            old_block_id: olds[0], new_block_id: news[0], source: 'AI_PROPOSED', membership_kind: 'EXPLICIT_1_1'}];
        if (olds.length === 1) return news.map(n => ({link_id: `ai:${region.id}:${olds[0]}:${n}`,
            old_block_id: olds[0], new_block_id: n, source: 'AI_PROPOSED', membership_kind: 'INHERITED_GROUP_1_N'}));
        if (news.length === 1) return olds.map(o => ({link_id: `ai:${region.id}:${o}:${news[0]}`,
            old_block_id: o, new_block_id: news[0], source: 'AI_PROPOSED', membership_kind: 'INHERITED_GROUP_N_1'}));
        return [];
    }

    function effectiveLinks(region, events) {
        let links = baseProposedLinks(region).map(l => ({...l}));
        for (const e of forRegion(events, region)) {
            if (e.event_type === 'ADD_BLOCK_LINK') {
                links.push({link_id: e.link_id, old_block_id: e.old_block_id, new_block_id: e.new_block_id,
                    source: 'HUMAN_MANUAL', membership_kind: 'HUMAN_DEFINED'});
            } else if (e.event_type === 'DELETE_BLOCK_LINK') {
                links = links.filter(l => l.link_id !== e.link_id
                    && !(l.old_block_id === e.old_block_id && l.new_block_id === e.new_block_id));
            } else if (e.event_type === 'REASSIGN_BLOCK_LINK') {
                links = links.map(l => (l.link_id === e.previous_link_id)
                    ? {link_id: e.link_id, old_block_id: e.old_block_id, new_block_id: e.new_block_id,
                        source: 'HUMAN_MANUAL', membership_kind: 'HUMAN_DEFINED'}
                    : l);
            }
        }
        const seen = new Set();
        return links.filter(l => {
            const k = edgeKey(l.old_block_id, l.new_block_id);
            if (seen.has(k)) return false;
            seen.add(k);
            return true;
        });
    }

    // validation.allowed_block_ids: members, or page context only for an explicitly EMPTY side.
    function allowedBlockIds(region, side) {
        const members = ids(side === 'OLD' ? region.old_blocks : region.new_blocks);
        if (members.length) return uniq(members);
        if ((region.membership_state || {})[side] === 'EMPTY') {
            return uniq(((region.pages || {})[side] || []).flatMap(p => ids(p.blocks)));
        }
        return [];
    }

    // validation.validate_block_link_event, as a result object instead of an exception.
    function preValidateLinkEvent({eventType, region, events, linkId, oldBlockId, newBlockId, previousLinkId = null}) {
        const fail = error => ({ok: false, error});
        if (!LINK_EVENT_TYPES.includes(eventType)) return fail('BAD_BLOCK_LINK_EVENT');
        const oldIds = allowedBlockIds(region, 'OLD'), newIds = allowedBlockIds(region, 'NEW');
        const effective = effectiveLinks(region, events);
        const pairs = new Set(effective.map(l => edgeKey(l.old_block_id, l.new_block_id)));
        let prevOld = null, prevNew = null, prevLink = previousLinkId;
        if (eventType === 'ADD_BLOCK_LINK' || eventType === 'REASSIGN_BLOCK_LINK') {
            if (newIds.includes(oldBlockId) && !oldIds.includes(oldBlockId)) return fail('WRONG_BLOCK_SIDE');
            if (oldIds.includes(newBlockId) && !newIds.includes(newBlockId)) return fail('WRONG_BLOCK_SIDE');
            if (!oldIds.includes(oldBlockId)) return fail('OLD_BLOCK_NOT_IN_REGION');
            if (!newIds.includes(newBlockId)) return fail('NEW_BLOCK_NOT_IN_REGION');
        }
        if (eventType === 'REASSIGN_BLOCK_LINK') {
            const source = effective.find(l => previousLinkId && l.link_id === previousLinkId);
            if (!source) return fail('BLOCK_LINK_NOT_FOUND');
            if (source.old_block_id === oldBlockId && source.new_block_id === newBlockId) {
                return {ok: true, noop: true, error: 'NO_CHANGE'};
            }
            if (pairs.has(edgeKey(oldBlockId, newBlockId))) return fail('BLOCK_LINK_ALREADY_EXISTS');
            prevOld = source.old_block_id;
            prevNew = source.new_block_id;
            prevLink = source.link_id;
        }
        if (eventType === 'ADD_BLOCK_LINK' && pairs.has(edgeKey(oldBlockId, newBlockId))) {
            return fail('BLOCK_LINK_ALREADY_EXISTS');
        }
        if (eventType === 'DELETE_BLOCK_LINK' && !effective.some(l => l.link_id === linkId
                || (l.old_block_id === oldBlockId && l.new_block_id === newBlockId))) {
            return fail('BLOCK_LINK_NOT_FOUND');
        }
        return {ok: true, previous_old_block_id: prevOld, previous_new_block_id: prevNew, previous_link_id: prevLink};
    }

    // ── Time: tz-aware ISO timestamps with microseconds (datetime.isoformat) ─
    const ISO = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?(Z|[+-]\d{2}:?\d{2})$/;
    function eventTime(event) {
        // Pending (not yet written) events sort after every stored event, in pending order.
        if (event && event.__pending) return {pending: event.__pending, s: 0, us: 0};
        const m = ISO.exec(String((event && event.timestamp) || ''));
        if (!m) return null;
        const [, y, mo, d, h, mi, s = '0', frac = '', tz] = m;
        const utc = Date.UTC(+y, +mo - 1, +d, +h, +mi, +s);
        if (Number.isNaN(utc)) return null;
        let offset = 0;
        if (tz !== 'Z') {
            const sign = tz[0] === '-' ? -1 : 1, digits = tz.slice(1).replace(':', '');
            offset = sign * (Number(digits.slice(0, 2)) * 3600 + Number(digits.slice(2, 4)) * 60);
        }
        return {pending: 0, s: utc / 1000 - offset, us: Number((frac + '000000').slice(0, 6))};
    }
    function compareTimes(a, b) {
        if (a.pending || b.pending) return (a.pending || 0) - (b.pending || 0);
        return a.s - b.s || a.us - b.us;
    }

    // ── Decisions (region verdict, last append wins) ────────────────────────
    function latestReview(region, reviews) {
        const rows = forRegion(reviews, region);
        return rows.length ? rows[rows.length - 1] : null;
    }
    function regionStatus(region, reviews) {
        const review = latestReview(region, reviews);
        return (review && review.status) || 'UNREVIEWED';
    }

    // Same order of checks as human_mapping_bridge._build: no review / UNCERTAIN → selection →
    // link newer than the review → CONFIRMED (anchor) or otherwise forbidden exact edge.
    function deriveEdgeStates(region, reviews, edits) {
        const review = latestReview(region, reviews);
        const reviewTime = review ? eventTime(review) : null;
        return effectiveLinks(region, edits).map(link => {
            const row = {...link, region_id: region.id, review_id: review ? review.review_id || null : null};
            if (!review) return {...row, state: EDGE.UNREVIEWED};
            if (review.status === 'HUMAN_UNCERTAIN') return {...row, state: EDGE.UNCERTAIN};
            if (!(review.old_block_ids || []).includes(link.old_block_id)
                    || !(review.new_block_ids || []).includes(link.new_block_id)) {
                return {...row, state: EDGE.OUTSIDE_SELECTION};
            }
            const origin = [...(edits || [])].reverse().find(e => e.link_id === link.link_id
                && e.region_id === region.id && e.event_type !== 'DELETE_BLOCK_LINK');
            if (origin) {
                const originTime = eventTime(origin);
                if (!originTime || !reviewTime) return {...row, state: EDGE.INVALID_TIMESTAMP};
                if (compareTimes(originTime, reviewTime) > 0) return {...row, state: EDGE.NOT_COVERED_NEWER};
            }
            return {...row, state: review.status === 'HUMAN_CONFIRMED' ? EDGE.ANCHOR : EDGE.FORBIDDEN};
        });
    }

    function regionConstraints(region, reviews, edits) {
        const states = deriveEdgeStates(region, reviews, edits);
        const pick = state => states.filter(s => s.state === state)
            .map(s => ({link_id: s.link_id, old_block_id: s.old_block_id, new_block_id: s.new_block_id, region_id: region.id}));
        return {anchors: pick(EDGE.ANCHOR), rejected: pick(EDGE.FORBIDDEN)};
    }

    // Run-level view: anchors and exact rejections of every region, plus edges that are an anchor
    // in one region and forbidden in another (the bridge refuses the whole snapshot then).
    function runConstraints(regions, reviews, edits) {
        const anchors = [], rejected = [];
        for (const region of regions || []) {
            const c = regionConstraints(region, reviews, edits);
            anchors.push(...c.anchors);
            rejected.push(...c.rejected);
        }
        const byEdge = new Map();
        for (const [kind, rows] of [['anchor', anchors], ['forbidden', rejected]]) {
            for (const r of rows) {
                const k = edgeKey(r.old_block_id, r.new_block_id);
                if (!byEdge.has(k)) byEdge.set(k, {old_block_id: r.old_block_id, new_block_id: r.new_block_id,
                    anchor_regions: [], forbidden_regions: []});
                byEdge.get(k)[kind + '_regions'].push(r.region_id);
            }
        }
        const conflicts = [...byEdge.values()].filter(c => c.anchor_regions.length && c.forbidden_regions.length);
        return {anchors, rejected, conflicts};
    }
    function crossRegionConflicts(regions, reviews, edits) { return runConstraints(regions, reviews, edits).conflicts; }

    // Per-event checks of human_mapping_bridge._build that need no run files. Any issue means the
    // history can never become a bridge snapshot (append-only). Order follows the bridge, so the
    // first issue carries the code the bridge would raise. blockIndex (optional): Set of 'SIDE|id'.
    function historyIssues({regions, reviews, edits, scope = null, blockIndex = null}) {
        const issues = [];
        const byId = new Map((regions || []).map(r => [r.id, r]));
        const seen = new Set();
        const add = (event, code) => issues.push({id: event.review_id || event.event_id || null, code});
        for (const event of [...(reviews || []), ...(edits || [])]) {
            if (scope && (event.object_id !== scope.object_id || event.pair_key !== scope.pair_id
                    || event.comparison_id !== scope.pair_id || event.run_id !== scope.run_id)) {
                add(event, 'HUMAN_EVENT_SCOPE_MISMATCH'); continue;
            }
            const region = byId.get(event.region_id);
            if (!region) { add(event, 'STALE_REGION'); continue; }
            const eid = event.review_id || event.event_id;
            if (typeof eid !== 'string' || !eid || seen.has(eid)) { add(event, 'DUPLICATE_OR_MISSING_EVENT_ID'); continue; }
            seen.add(eid);
            if (!eventTime(event)) { add(event, 'INVALID_EVENT_TIMESTAMP'); continue; }
            for (const side of SIDES) {
                const key = side.toLowerCase();
                const sideIds = 'review_id' in event ? event[key + '_block_ids'] : [event[key + '_block_id']];
                if (!Array.isArray(sideIds) || !sideIds.length) { add(event, 'EMPTY_REVIEW_ENDPOINTS'); break; }
                if (blockIndex && !sideIds.every(id => blockIndex.has(side + '|' + id))) {
                    add(event, 'UNKNOWN_OR_WRONG_SIDE_BLOCK'); break;
                }
                const allowed = allowedBlockIds(region, side);
                if (!sideIds.every(id => allowed.includes(id))) { add(event, 'BLOCK_OUTSIDE_REGION'); break; }
            }
        }
        if (issues.length) return issues;
        const replayed = [];
        for (const event of edits || []) {
            const res = preValidateLinkEvent({eventType: event.event_type, region: byId.get(event.region_id),
                events: replayed, linkId: event.link_id, oldBlockId: event.old_block_id,
                newBlockId: event.new_block_id, previousLinkId: event.previous_link_id || null});
            if (!res.ok) { add(event, 'INVALID_SOURCE_OR_HISTORY'); return issues; }
            replayed.push(event);
        }
        for (const review of reviews || []) {
            if (!REVIEW_STATUSES.includes(review.status)) { add(review, 'INVALID_STATUS'); return issues; }
        }
        const conflicts = crossRegionConflicts(regions, reviews, edits);
        for (const c of conflicts) issues.push({id: null, code: 'CONFIRMED_AND_REJECTED_EXACT_EDGE', edge: c});
        return issues;
    }

    // ── Planning a region verdict ───────────────────────────────────────────
    // Simulates the new review (and links added right before it) as newer than every stored
    // event, then compares edge states before/after. Nothing is written.
    function planReview(region, reviews, edits, {status, oldIds, newIds, adds = [], deletes = [], desired = null}) {
        const pendingEdits = [...(edits || [])];
        let rank = 1;
        for (const d of deletes) pendingEdits.push({...d, event_type: 'DELETE_BLOCK_LINK', region_id: region.id, __pending: rank++});
        for (const a of adds) pendingEdits.push({link_id: '__planned__:' + edgeKey(a.old_block_id, a.new_block_id), ...a,
            event_type: 'ADD_BLOCK_LINK', region_id: region.id, __pending: rank++});
        const prior = latestReview(region, reviews);
        const review = {review_id: '__planned__', region_id: region.id, status,
            old_block_ids: uniq(oldIds), new_block_ids: uniq(newIds), __pending: rank};
        const before = deriveEdgeStates(region, reviews, edits);
        const after = deriveEdgeStates(region, [...(reviews || []), review], pendingEdits);
        const stateOf = (rows, k) => (rows.find(r => edgeKey(r.old_block_id, r.new_block_id) === k) || {}).state || null;
        const keys = uniq([...before, ...after].map(r => edgeKey(r.old_block_id, r.new_block_id)));
        const changes = keys.map(k => {
            const row = after.find(r => edgeKey(r.old_block_id, r.new_block_id) === k)
                || before.find(r => edgeKey(r.old_block_id, r.new_block_id) === k);
            return {old_block_id: row.old_block_id, new_block_id: row.new_block_id, link_id: row.link_id,
                before: stateOf(before, k), after: stateOf(after, k)};
        }).filter(c => c.before !== c.after);
        const constrainedAfter = after.filter(r => CONSTRAINED.includes(r.state));
        const desiredKeys = desired ? new Set(desired.map(e => edgeKey(e.old_block_id, e.new_block_id))) : null;
        return {
            status,
            replaces: prior ? {review_id: prior.review_id || null, status: prior.status, timestamp: prior.timestamp || null} : null,
            anchors: after.filter(r => r.state === EDGE.ANCHOR),
            forbidden: after.filter(r => r.state === EDGE.FORBIDDEN),
            lost: changes.filter(c => CONSTRAINED.includes(c.before) && !CONSTRAINED.includes(c.after)),
            flipped: changes.filter(c => CONSTRAINED.includes(c.before) && CONSTRAINED.includes(c.after)),
            extra: desiredKeys ? constrainedAfter.filter(r => !desiredKeys.has(edgeKey(r.old_block_id, r.new_block_id))) : [],
            changes,
            zeroConstraint: status !== 'HUMAN_UNCERTAIN' && !constrainedAfter.length,
            before, after,
        };
    }

    function endpoints(edges) {
        return {oldIds: uniq(edges.map(e => e.old_block_id)), newIds: uniq(edges.map(e => e.new_block_id))};
    }

    // Turns a user intent into ordinary HM events: link events (ADD/DELETE/REASSIGN) and at most one
    // region review. Returns {kind:'EVENTS'|'CHOICE'|'ERROR', ...}. newLinkId() is injected (human:<uuid>).
    function compileDecision(region, reviews, edits, intent, {newLinkId} = {}) {
        const error = code => ({kind: 'ERROR', error: code});
        const makeId = () => {
            if (!newLinkId) throw new CompileError('LINK_ID_FACTORY_REQUIRED');
            return newLinkId();
        };
        if (EDGE_INTENTS.includes(intent.kind) && !(intent.edge && intent.edge.old_block_id && intent.edge.new_block_id)) {
            return error('EDGE_REQUIRED');
        }
        try {
            return compileIntent(region, reviews, edits, intent, latestReview(region, reviews), error, makeId);
        } catch (e) {
            if (e instanceof CompileError) return error(e.code);
            throw e;
        }
    }
    const EDGE_INTENTS = ['CONFIRM_LINK', 'REJECT_LINK', 'UNCERTAIN_LINK'];
    class CompileError extends Error {
        constructor(code) { super(code); this.code = code; }
    }
    function compileIntent(region, reviews, edits, intent, prior, error, makeId) {
        const states = deriveEdgeStates(region, reviews, edits);
        const effective = effectiveLinks(region, edits);
        const isEffective = (o, n) => effective.some(l => l.old_block_id === o && l.new_block_id === n);
        const inState = (...wanted) => states.filter(s => wanted.includes(s.state));
        const allowed = {OLD: allowedBlockIds(region, 'OLD'), NEW: allowedBlockIds(region, 'NEW')};
        const outside = (oldIds, newIds) => oldIds.some(id => !allowed.OLD.includes(id)) || newIds.some(id => !allowed.NEW.includes(id));
        const review = (status, edges) => {
            const {oldIds, newIds} = endpoints(edges);
            return {region_id: region.id, status, old_block_ids: oldIds, new_block_ids: newIds,
                previous_review_id: prior ? prior.review_id || null : null};
        };
        const done = (linkEvents, rev, planArgs) => ({kind: 'EVENTS', linkEvents, review: rev,
            plan: rev ? planReview(region, reviews, edits, {status: rev.status, oldIds: rev.old_block_ids,
                newIds: rev.new_block_ids, ...planArgs}) : null});
        const addEvent = (o, n) => ({event_type: 'ADD_BLOCK_LINK', region_id: region.id, link_id: makeId(),
            old_block_id: o, new_block_id: n, previous_old_block_id: null, previous_new_block_id: null});
        const edge = intent.edge ? {old_block_id: intent.edge.old_block_id, new_block_id: intent.edge.new_block_id} : null;
        if (edge && outside([edge.old_block_id], [edge.new_block_id])) return error('BLOCK_NOT_IN_REGION');
        const addsFor = e => (isEffective(e.old_block_id, e.new_block_id) ? [] : [e]);

        switch (intent.kind) {
            case 'CONNECT': {
                const pairs = intent.pairs || [];
                if (!pairs.length) return error('NO_PAIRS');
                if (pairs.length > MAX_EDGES_PER_ACTION) return error('TOO_MANY_EDGES');
                const replayed = [...(edits || [])], events = [], duplicates = [];
                for (const [o, n] of pairs) {
                    const ev = addEvent(o, n);
                    const res = preValidateLinkEvent({eventType: ev.event_type, region, events: replayed,
                        linkId: ev.link_id, oldBlockId: o, newBlockId: n});
                    if (!res.ok && res.error === 'BLOCK_LINK_ALREADY_EXISTS') { duplicates.push([o, n]); continue; }
                    if (!res.ok) return error(res.error);
                    events.push(ev);
                    replayed.push(ev);
                }
                return {...done(events, null), duplicates};
            }
            case 'CONFIRM_LINK': {
                const anchors = prior && prior.status === 'HUMAN_CONFIRMED' ? inState(EDGE.ANCHOR) : [];
                const desired = uniqEdges([...anchors, edge]);
                const adds = addsFor(edge);
                return done(adds.map(a => addEvent(a.old_block_id, a.new_block_id)), review('HUMAN_CONFIRMED', desired), {adds, desired});
            }
            case 'REJECT_LINK': {
                const otherAnchors = prior && prior.status === 'HUMAN_CONFIRMED'
                    ? inState(EDGE.ANCHOR).filter(s => !sameEdge(s, edge)) : [];
                if (otherAnchors.length && !intent.choice) {
                    return {kind: 'CHOICE', choice: 'REJECT_WITH_ANCHORS', anchors: otherAnchors,
                        options: ['DELETE_ONLY', 'REJECT_REPLACING', 'CANCEL']};
                }
                if (intent.choice === 'CANCEL') return {kind: 'EVENTS', linkEvents: [], review: null, plan: null};
                if (intent.choice === 'DELETE_ONLY') {
                    const link = effective.find(l => sameEdge(l, edge));
                    if (!link) return error('BLOCK_LINK_NOT_FOUND');
                    return done([{event_type: 'DELETE_BLOCK_LINK', region_id: region.id, link_id: link.link_id,
                        old_block_id: link.old_block_id, new_block_id: link.new_block_id,
                        previous_old_block_id: link.old_block_id, previous_new_block_id: link.new_block_id}], null);
                }
                const forbidden = prior && prior.status === 'HUMAN_REJECTED' ? inState(EDGE.FORBIDDEN) : [];
                const desired = uniqEdges([...forbidden, edge]);
                const adds = addsFor(edge);
                return done(adds.map(a => addEvent(a.old_block_id, a.new_block_id)), review('HUMAN_REJECTED', desired), {adds, desired});
            }
            case 'UNCERTAIN_LINK': {
                const adds = addsFor(edge);
                const addEvents = adds.map(a => addEvent(a.old_block_id, a.new_block_id));
                if (!prior || prior.status === 'HUMAN_UNCERTAIN' || intent.choice === 'REGION_UNCERTAIN') {
                    const base = prior ? prior : {old_block_ids: [], new_block_ids: []};
                    const rev = {region_id: region.id, status: 'HUMAN_UNCERTAIN',
                        old_block_ids: uniq([...(base.old_block_ids || []), edge.old_block_id]),
                        new_block_ids: uniq([...(base.new_block_ids || []), edge.new_block_id]),
                        previous_review_id: prior ? prior.review_id || null : null};
                    return done(addEvents, rev, {adds});
                }
                // Exclude the edge from a CONFIRMED/REJECTED region verdict by repeating it without the edge.
                const covered = inState(EDGE.ANCHOR, EDGE.FORBIDDEN).filter(s => !sameEdge(s, edge));
                if (!covered.length) {
                    return {kind: 'CHOICE', choice: 'UNCERTAIN_WHOLE_REGION', options: ['REGION_UNCERTAIN', 'CANCEL']};
                }
                const result = done([], review(prior.status, covered), {desired: covered});
                const stillConstrained = result.plan.after.some(s => sameEdge(s, edge) && CONSTRAINED.includes(s.state));
                return {...result, cannotExclude: stillConstrained};
            }
            case 'RECONFIRM': {
                if (!prior || prior.status === 'HUMAN_UNCERTAIN') return error('NOTHING_TO_RECONFIRM');
                const desired = inState(EDGE.ANCHOR, EDGE.FORBIDDEN, EDGE.NOT_COVERED_NEWER);
                if (!desired.length) return error('NOTHING_TO_RECONFIRM');
                return done([], review(prior.status, desired), {desired});
            }
            case 'REGION_DECISION': {
                if (!REVIEW_STATUSES.includes(intent.status)) return error('INVALID_STATUS');
                const oldIds = (intent.oldIds && intent.oldIds.length) ? uniq(intent.oldIds) : ids(region.old_blocks);
                const newIds = (intent.newIds && intent.newIds.length) ? uniq(intent.newIds) : ids(region.new_blocks);
                if (!oldIds.length || !newIds.length) return error('EMPTY_SIDE_NEEDS_SELECTION');
                if (outside(oldIds, newIds)) return error('SELECTION_OUTSIDE_REGION');
                const rev = {region_id: region.id, status: intent.status, old_block_ids: oldIds, new_block_ids: newIds,
                    previous_review_id: prior ? prior.review_id || null : null};
                return done([], rev, {});
            }
            case 'DELETE_LINK': {
                const link = effective.find(l => l.link_id === intent.linkId);
                if (!link) return error('BLOCK_LINK_NOT_FOUND');
                return done([{event_type: 'DELETE_BLOCK_LINK', region_id: region.id, link_id: link.link_id,
                    old_block_id: link.old_block_id, new_block_id: link.new_block_id,
                    previous_old_block_id: link.old_block_id, previous_new_block_id: link.new_block_id}], null);
            }
            case 'REASSIGN_LINK': {
                const link = effective.find(l => l.link_id === intent.linkId);
                if (!link) return error('BLOCK_LINK_NOT_FOUND');
                const nextOld = intent.side === 'OLD' ? intent.blockId : link.old_block_id;
                const nextNew = intent.side === 'NEW' ? intent.blockId : link.new_block_id;
                const ev = {event_type: 'REASSIGN_BLOCK_LINK', region_id: region.id, link_id: makeId(),
                    old_block_id: nextOld, new_block_id: nextNew, previous_old_block_id: link.old_block_id,
                    previous_new_block_id: link.new_block_id, previous_link_id: link.link_id};
                const res = preValidateLinkEvent({eventType: ev.event_type, region, events: edits, linkId: ev.link_id,
                    oldBlockId: nextOld, newBlockId: nextNew, previousLinkId: link.link_id});
                if (res.ok && res.noop) return {kind: 'EVENTS', linkEvents: [], review: null, plan: null, noop: true};
                if (!res.ok) return error(res.error);
                return done([ev], null);
            }
            default:
                return error('UNKNOWN_INTENT');
        }
    }
    function sameEdge(a, b) { return a.old_block_id === b.old_block_id && a.new_block_id === b.new_block_id; }
    function uniqEdges(edges) {
        const seen = new Set();
        return edges.filter(e => {
            const k = edgeKey(e.old_block_id, e.new_block_id);
            if (seen.has(k)) return false;
            seen.add(k);
            return true;
        }).map(e => ({old_block_id: e.old_block_id, new_block_id: e.new_block_id}));
    }

    // ── Page lens (derived, never stored) ───────────────────────────────────
    function sortedPages(pages) { return uniq((pages || []).map(Number)).sort((a, b) => a - b); }
    // Page-set composition key of a sheet-map row; stable when link ids are reissued.
    function groupKey(leftPages, rightPages) {
        return 'L' + sortedPages(leftPages).join(',') + '|R' + sortedPages(rightPages).join(',');
    }
    // Pages of a region side: ui_data (pages[side][].page) or region index (pages[side] = numbers).
    function regionPages(region, side) {
        return sortedPages(((region.pages || {})[side] || []).map(p => (typeof p === 'object' ? p.page : p)));
    }
    function rel(region, row) {
        const oldPages = regionPages(region, 'OLD'), newPages = regionPages(region, 'NEW');
        const left = sortedPages(row.leftPages), right = sortedPages(row.rightPages);
        const o = oldPages.filter(p => left.includes(p)), n = newPages.filter(p => right.includes(p));
        if (!o.length && !n.length) return 'NONE';
        if (o.length && n.length && oldPages.every(p => left.includes(p)) && newPages.every(p => right.includes(p))) return 'INSIDE';
        if (o.length && n.length) return 'BOTH_PARTIAL';
        return o.length ? 'OLD_ONLY' : 'NEW_ONLY';
    }
    function isTwoSided(row) { return !!((row.leftPages || []).length && (row.rightPages || []).length); }
    // Pinned row under the sheet map: regions touching no two-sided row at all.
    function untouchedRegions(regions, rows) {
        const twoSided = (rows || []).filter(isTwoSided);
        return (regions || []).filter(r => twoSided.every(row => rel(r, row) === 'NONE'));
    }
    // «Без общей пары листов»: no two-sided row holds both sides of the region.
    function unpairedRegions(regions, rows) {
        const twoSided = (rows || []).filter(isTwoSided);
        return (regions || []).filter(r => !twoSided.some(row => ['INSIDE', 'BOTH_PARTIAL'].includes(rel(r, row))));
    }
    // Placement of a decided link (pages of its ends) relative to the SAVED sheet links.
    function placementClass(oldPage, newPage, savedLinks) {
        const links = (savedLinks || []).map(l => ({left: sortedPages(l.left_pages), right: sortedPages(l.right_pages)}));
        const rows = links.filter(l => l.left.includes(+oldPage) && l.right.includes(+newPage)).map(l => groupKey(l.left, l.right));
        if (rows.length) return {kind: 'IN_ROW', rows: uniq(rows).sort()};
        const oldMapped = links.some(l => l.left.includes(+oldPage)), newMapped = links.some(l => l.right.includes(+newPage));
        return {kind: oldMapped && newMapped ? 'CROSS_ROWS' : 'OFF_MAP', rows: []};
    }
    function placementChanged(a, b) {
        return a.kind !== b.kind || a.rows.join(';') !== b.rows.join(';');
    }
    function blockPage(region, side, blockId) {
        const members = side === 'OLD' ? region.old_blocks : region.new_blocks;
        const member = (members || []).find(b => b.id === blockId);
        if (member && member.page != null) return member.page;
        for (const page of ((region.pages || {})[side] || [])) {
            if (typeof page === 'object' && (page.blocks || []).some(b => b.id === blockId)) return page.page;
        }
        return null;
    }

    // ── Geometry and viewer state ───────────────────────────────────────────
    // HM overlay: normalized bbox [x0, y0, x1, y1] → CSS percentages of the page raster.
    function bboxPercent(bbox) {
        const [x, y, w, h] = bbox;
        return {left: x * 100, top: y * 100, width: (w - x) * 100, height: (h - y) * 100};
    }
    function bboxCenter(bbox, box) {
        const [x0, y0, x1, y1] = bbox;
        return {x: box.left + (x0 + x1) / 2 * box.width, y: box.top + (y0 + y1) / 2 * box.height};
    }
    // Same path as HM pathPair: a straight segment.
    function linkPath(a, b) { return `M ${a.x} ${a.y} L ${b.x} ${b.y}`; }
    // Liang–Barsky clip of segment a→b to a panel rectangle; null when fully outside.
    function clipSegment(a, b, rect) {
        const dx = b.x - a.x, dy = b.y - a.y;
        let t0 = 0, t1 = 1;
        const edges = [[-dx, a.x - rect.left], [dx, rect.left + rect.width - a.x],
            [-dy, a.y - rect.top], [dy, rect.top + rect.height - a.y]];
        for (const [p, q] of edges) {
            if (p === 0) { if (q < 0) return null; continue; }
            const t = q / p;
            if (p < 0) { if (t > t1) return null; if (t > t0) t0 = t; } else { if (t < t0) return null; if (t < t1) t1 = t; }
        }
        return {a: {x: a.x + t0 * dx, y: a.y + t0 * dy}, b: {x: a.x + t1 * dx, y: a.y + t1 * dy},
            clippedA: t0 > 0, clippedB: t1 < 1};
    }
    // Independent per-side pan/zoom as in HM: lock blocks panning only; zoom range 0.6–1.8.
    function createView() { return {x: 0, y: 0, z: 1, locked: false}; }
    function zoomView(view, z) { return {...view, z: Math.min(ZOOM_MAX, Math.max(ZOOM_MIN, Number(z) || 1))}; }
    function panView(view, dx, dy) { return view.locked ? view : {...view, x: view.x + dx, y: view.y + dy}; }
    function resetView(view) { return {...view, x: 0, y: 0, z: 1}; }
    function lockView(view, locked) { return {...view, locked: !!locked}; }

    return Object.freeze({
        SIDES, REVIEW_STATUSES, LINK_EVENT_TYPES, EDGE, ZOOM_MIN, ZOOM_MAX, MAX_EDGES_PER_ACTION,
        edgeKey, baseProposedLinks, effectiveLinks, allowedBlockIds, preValidateLinkEvent,
        eventTime, compareTimes, latestReview, regionStatus, deriveEdgeStates, regionConstraints,
        runConstraints, crossRegionConflicts, historyIssues, planReview, compileDecision,
        groupKey, regionPages, rel, isTwoSided, untouchedRegions, unpairedRegions,
        placementClass, placementChanged, blockPage,
        bboxPercent, bboxCenter, linkPath, clipSegment,
        createView, zoomView, panView, resetView, lockView,
    });
}));
