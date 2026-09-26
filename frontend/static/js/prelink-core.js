(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.PrelinkCore = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';
    // Pure rules of the pre-analysis human prelinks in stage 2. No DOM, no fetch.
    // A prelink is ONE group {old_blocks, new_blocks}: 1→1 and 1→N / N→1 are the edges the person drew
    // (a star), N↔N has no pairs at all — it is drawn as spokes to one node, never as a Cartesian grid.
    // Prelinks are never shown to a model; after an analysis the server reconciles the run's frozen copy
    // with the AI regions (GET …/prelink-reconciliation) and a person decides explicitly: the decision
    // compiles into ordinary Human Mapping events with the same semantics as HumanMappingCore's
    // CONFIRM_LINK / REJECT_LINK / UNCERTAIN_LINK, generalised to the edges of one prelink.

    const CARDINALITY = {'1:1': '1→1', '1:N': '1→N', 'N:1': 'N→1', 'N:N': 'N↔N'};
    const STATE = {
        MATCHED: {glyph: '✓', label: 'Совпало'},
        PARTIAL_MATCH: {glyph: '◐', label: 'Частично'},
        CONFLICT: {glyph: '⚠', label: 'Расходится'},
        UNRESOLVED: {glyph: '?', label: 'Не определено'},
        NOT_EVALUATED: {glyph: '⌀', label: 'Не сверялась'},
    };
    const STATES = Object.keys(STATE);
    const VALIDITY = {
        VALID: '',
        REVALIDATED: 'Распознавание обновлено — блоки связи проверены и не изменились.',
        STALE_TEXT: 'Текст блока изменился после создания связи. Проверьте и подтвердите связь заново.',
        STALE_BLOCKS: 'Блок исчез или изменился после обновления распознавания — связь выключена.',
        STALE_PDF: 'Документ изменился после создания связи — связь выключена.',
        SOURCE_UNAVAILABLE: 'Нет распознавания стороны — связь недоступна.',
    };
    const REASON = {
        CONTEXT_ONLY: 'один из блоков есть только на страницах региона, но ИИ не отметил его частью региона',
        MEMBER_OF_INCOMPLETE_REGION: 'блок входит только в регион, у которого одна сторона пуста',
        BLOCK_UNPLACED: 'ИИ не включил блок ни в один регион — сверить не с чем',
        EXCLUDED_STALE: 'устарела до запуска анализа',
        SOURCE_MISMATCH: 'распознавание изменилось во время запуска — блоки анализа другие',
        RUN_NOT_READY: 'анализ не завершён',
        HM_UNAVAILABLE: 'данные смысловых блоков анализа недоступны',
        GROUP_HAS_NO_PAIRS: 'Группа N↔N не задаёт пар блоков. Подтвердите связи ИИ между ними или соедините пары вручную.',
        NO_REGION_ALLOWS_THE_LINK: 'ИИ разнёс эти блоки по разным регионам. Связь между регионами записать нельзя.',
    };
    const ERROR = {
        PRELINK_SIDE_EMPTY: 'Выберите хотя бы один блок OLD и один блок NEW.',
        PRELINK_BLOCK_NOT_FOUND: 'Блок не найден в распознавании — обновите страницу.',
        PRELINK_WRONG_SIDE: 'Блок другой стороны нельзя поставить на эту сторону.',
        PRELINK_STAMP_NOT_ALLOWED: 'Штамп нельзя связать — для соответствия листов есть карта листов.',
        PRELINK_TOO_MANY_BLOCKS: 'В одной связи не больше 12 блоков на сторону.',
        PRELINK_LIMIT_REACHED: 'Достигнут предел предварительных связей пары (60 связей, 400 блоков).',
        PRELINK_DUPLICATE_BLOCK: 'Блок выбран дважды.',
        PRELINK_DUPLICATE: 'Такая связь уже есть.',
        PRELINK_NOTE_TOO_LONG: 'Заметка длиннее 500 символов.',
        PRELINK_REVISION_CONFLICT: 'Связи изменены в другой вкладке. Показано актуальное состояние — повторите действие.',
        PRELINK_NOT_FOUND: 'Связь уже удалена — показано актуальное состояние.',
        PRELINK_DRAFTS_DISABLED: 'Учёт предварительных связей выключен.',
        PRELINK_DRAFTS_INVALID: 'Файл предварительных связей повреждён — изменения невозможны. Обратитесь к администратору.',
        SOURCE_BLOCKS_UNAVAILABLE: 'Нет распознавания стороны — предварительные связи недоступны.',
        PRELINK_SOURCE_CHANGED: 'Документ изменился после создания связи. Эту связь изменить нельзя — удалите её '
            + 'и при необходимости создайте новую связь на текущей версии.',
    };
    // Why an UNRESOLVED link cannot be placed (technical reason under the neutral explanation).
    const UNRESOLVED_REASON = {
        BLOCK_UNPLACED: 'блок не вошёл ни в один смысловой регион',
        CONTEXT_ONLY: 'блок есть на страницах региона, но ИИ не отметил его частью региона',
        MEMBER_OF_INCOMPLETE_REGION: 'блок входит только в регион, у которого одна сторона не размещена',
    };
    // Why a link was stale at launch (the card of an excluded link).
    const STALE_REASON = {
        STALE_PDF: 'документ (PDF или его версия) изменился после создания связи',
        STALE_BLOCKS: 'блок исчез или изменил положение либо тип после обновления распознавания',
        STALE_TEXT: 'текст блока изменился после создания связи',
        SOURCE_UNAVAILABLE: 'распознавание стороны было недоступно',
    };
    const TEXT = {
        D1: 'Предварительные связи — ваши отметки соответствия блоков. ИИ их не видит; после анализа мы сравним их с результатом, а решение примете вы.',
        D2: (o, n, kind) => `Будет создана связь ${kind} (OLD ${o} · NEW ${n}).`
            + (kind === 'N↔N' ? ' Группа N↔N не задаёт пар — если важны пары, связывайте по одной.' : ''),
        D4: rev => `Идёт анализ по состоянию ред. ${rev}. Правки сохранятся и будут сравнены со следующим анализом.`,
        D6: 'Учёт предварительных связей выключен. Связи сохранены; их можно посмотреть и удалить.',
        SAVED: (rev, time) => `Сохранено · ред. ${rev} · ${time}`,
        DELETED: label => `Связь ${label} удалена.`,
        L1: (n, stale) => `Предварительные связи: ${n}. ИИ их не видит — после анализа мы сравним их с результатом.`
            + (stale ? ` Устарели: ${stale} — сверяться не будут.` : ''),
        R1: (run, rev, c) => `Ваши связи из анализа ${run}` + (rev != null ? ` (ред. ${rev})` : '') + ': '
            + `✓ совпало ${c.MATCHED} · ◐ частично ${c.PARTIAL_MATCH} · ⚠ расходится ${c.CONFLICT} · `
            + `? не определено ${c.UNRESOLVED} · ⌀ не сверялись ${c.NOT_EVALUATED}`,
        R2: 'Связи не влияли на анализ — ИИ их не видел. Это сравнение его результата с вашими связями.',
        R_CHANGED: d => `После анализа связи менялись (+${d.added}, −${d.removed}, изменено ${d.changed}). Здесь — состояние на момент запуска.`,
        R_MANY: n => `ИИ держит эти блоки вместе в ${n} регионах — выберите, куда перенести.`,
        R_CROSS: 'Связь между регионами записать нельзя.',
        R_UNRESOLVED: 'По результату анализа эту связь нельзя надёжно сопоставить с одним смысловым регионом.',
        R_UNRESOLVED_REASON: code => 'Причина: ' + (UNRESOLVED_REASON[code] || 'недостаточно данных для однозначного сопоставления') + '.',
        R_STALE: 'Связь устарела',
        R_STALE_REASON: code => 'Причина: ' + (STALE_REASON[code] || 'источник изменился до запуска анализа') + '.',
        R_STALE_ENDS: 'Блоки указаны так, как они были отмечены до анализа. Связь не сверялась и не переносится.',
        R_STALE_NO_ENDS: 'Состав связи в снимке запуска не сохранён — блоки показать нельзя.',
        D_SOURCE_CHANGED: 'Документ изменился после создания связи: эту связь нельзя изменить или вернуть к текущей версии. '
            + 'Удалите её; такую же связь для текущей версии создайте заново кнопкой «Связать».',
        R_AGREED: 'Вы согласились с ИИ. В Human Mapping ничего не записано.',
        COMMENT: (label, run) => `Из предварительной связи ${label} (анализ ${run})`,
    };

    function cardinality(olds, news) {
        if (olds === 1 && news === 1) return '1:1';
        if (olds === 1) return '1:N';
        if (news === 1) return 'N:1';
        return 'N:N';
    }
    const cardinalityLabel = code => CARDINALITY[code] || code || '';
    const ids = (list) => (list || []).map(b => (typeof b === 'string' ? b : b.block_id));

    // The edges the person drew: 1→1 one, 1→N / N→1 a star; N↔N none (mirrors prelink_reconciliation.prelink_edges).
    function edgesOf(item) {
        const olds = ids(item.old_blocks), news = ids(item.new_blocks);
        if (olds.length === 1) return news.map(n => ({old_block_id: olds[0], new_block_id: n}));
        if (news.length === 1) return olds.map(o => ({old_block_id: o, new_block_id: news[0]}));
        return [];
    }

    // Line geometry of prelinks over the two panels: segments between visible block centres; for N↔N a node in
    // the gap between the panels (x) at the mean height of the visible ends (y) with one spoke per block.
    // An end scrolled out of its panel is pinned to the panel's border along the line (as the AI lines are),
    // so a dashed line never runs over the rest of the page.
    function pin(p, toward, rect) {
        if (!rect) return p;
        const right = rect.left + rect.width, bottom = rect.top + rect.height;
        if (p.x >= rect.left && p.x <= right && p.y >= rect.top && p.y <= bottom) return p;
        const dx = toward.x - p.x, dy = toward.y - p.y;
        let t0 = 0, t1 = 1;
        for (const [a, b] of [[-dx, p.x - rect.left], [dx, right - p.x], [-dy, p.y - rect.top], [dy, bottom - p.y]]) {
            if (a === 0) { if (b < 0) { t0 = 2; break; } continue; }
            const t = b / a;
            if (a < 0) t0 = Math.max(t0, t); else t1 = Math.min(t1, t);
        }
        if (t0 <= t1 && t0 <= 1) return {x: p.x + t0 * dx, y: p.y + t0 * dy};
        return {x: Math.min(right, Math.max(rect.left, p.x)), y: Math.min(bottom - 6, Math.max(rect.top + 6, p.y))};
    }

    function composeLines(items, centers, rects) {
        const out = [];
        const own = side => (rects && rects[side]) || null;
        const gapX = rects && rects.OLD && rects.NEW
            ? (rects.OLD.left + rects.OLD.width + rects.NEW.left) / 2 : null;
        for (const item of items || []) {
            const ends = [...ids(item.old_blocks).map(id => ['OLD', id]), ...ids(item.new_blocks).map(id => ['NEW', id])]
                .map(([side, id]) => ({side, id, at: centers[side] && centers[side].get(id)})).filter(e => e.at);
            const edges = edgesOf(item);
            const segments = [];
            let node = null;
            if (edges.length) {
                for (const e of edges) {
                    const a = centers.OLD && centers.OLD.get(e.old_block_id), b = centers.NEW && centers.NEW.get(e.new_block_id);
                    if (!a || !b) continue;
                    const p = pin(a, b, own('OLD')), q = pin(b, a, own('NEW'));
                    segments.push({d: `M ${p.x} ${p.y} L ${q.x} ${q.y}`, mid: {x: (p.x + q.x) / 2, y: (p.y + q.y) / 2}});
                }
            } else if (ends.length >= 2 && ends.some(e => e.side === 'OLD') && ends.some(e => e.side === 'NEW')) {
                const x = gapX !== null ? gapX : ends.reduce((s, e) => s + e.at.x, 0) / ends.length;
                let y = ends.reduce((s, e) => s + e.at.y, 0) / ends.length;
                const shown = ['OLD', 'NEW'].map(own).filter(Boolean);
                if (shown.length) {   // the node stays where both panels are seen, however far the ends are scrolled
                    const top = Math.max(...shown.map(r => r.top)) + 6, bottom = Math.min(...shown.map(r => r.top + r.height)) - 6;
                    if (top <= bottom) y = Math.min(bottom, Math.max(top, y));
                }
                node = {x, y};
                for (const e of ends) {
                    const p = pin(e.at, node, own(e.side));
                    segments.push({d: `M ${p.x} ${p.y} L ${node.x} ${node.y}`, mid: node});
                }
            }
            if (!segments.length) continue;
            out.push({key: item.prelink_id, item, segments, node, mid: node || segments[0].mid});
        }
        return out;
    }

    // Decision of a person on the edges of one prelink in one region → ordinary HM events (one queue):
    // missing edges become ADD_BLOCK_LINK with the server's deterministic link ids, plus one region review.
    // CONFIRM: prior anchors of a confirmed region stay, the edges join them (as CONFIRM_LINK).
    // REJECT: prior exact rejections of a rejected region stay; over a confirmed region with other anchors a
    // choice is required, because the region has one verdict (as REJECT_LINK / REJECT_REPLACING).
    // UNCERTAIN: without a verdict — «не уверен» over the edges; over a verdict the edges are excluded by
    // repeating it without them, or the whole region becomes uncertain (as UNCERTAIN_LINK).
    function compilePromotion(Core, region, reviews, edits, {edges, status, linkIdOf, choice = ''}) {
        const error = code => ({kind: 'ERROR', error: code});
        if (!region) return error('REGION_NOT_FOUND');
        if (!edges || !edges.length) return error('NO_EDGES');
        const allowed = {OLD: Core.allowedBlockIds(region, 'OLD'), NEW: Core.allowedBlockIds(region, 'NEW')};
        if (edges.some(e => !allowed.OLD.includes(e.old_block_id) || !allowed.NEW.includes(e.new_block_id))) {
            return error('BLOCK_NOT_IN_REGION');
        }
        const key = e => Core.edgeKey(e.old_block_id, e.new_block_id);
        const uniq = list => [...new Map(list.map(e => [key(e), {old_block_id: e.old_block_id, new_block_id: e.new_block_id}])).values()];
        const states = Core.deriveEdgeStates(region, reviews, edits);
        const effective = Core.effectiveLinks(region, edits);
        const inState = (...wanted) => states.filter(s => wanted.includes(s.state));
        const prior = Core.latestReview(region, reviews);
        const wanted = new Set(edges.map(key));
        const adds = uniq(edges).filter(e => !effective.some(l => key(l) === key(e)));
        const addEvents = adds.map(e => ({event_type: 'ADD_BLOCK_LINK', region_id: region.id, link_id: linkIdOf(e),
            old_block_id: e.old_block_id, new_block_id: e.new_block_id, previous_old_block_id: null, previous_new_block_id: null}));
        const reviewOf = (reviewStatus, desired) => {
            const list = uniq(desired);
            return {region_id: region.id, status: reviewStatus,
                old_block_ids: [...new Set(list.map(e => e.old_block_id))], new_block_ids: [...new Set(list.map(e => e.new_block_id))],
                previous_review_id: prior ? prior.review_id || null : null};
        };
        // As the core's done(): the plan simulates the plain added edges, the events carry the real link ids.
        const done = (linkEvents, review, desired) => ({kind: 'EVENTS', linkEvents, review,
            plan: Core.planReview(region, reviews, edits, {status: review.status, oldIds: review.old_block_ids,
                newIds: review.new_block_ids, adds: linkEvents.length ? adds : [], desired})});
        if (status === 'HUMAN_CONFIRMED') {
            const anchors = prior && prior.status === 'HUMAN_CONFIRMED' ? inState('ANCHOR') : [];
            const desired = uniq([...anchors, ...edges]);
            return done(addEvents, reviewOf('HUMAN_CONFIRMED', desired), desired);
        }
        if (status === 'HUMAN_REJECTED') {
            const others = prior && prior.status === 'HUMAN_CONFIRMED' ? inState('ANCHOR').filter(s => !wanted.has(key(s))) : [];
            if (others.length && choice !== 'REJECT_REPLACING') {
                return {kind: 'CHOICE', choice: 'REJECT_WITH_ANCHORS', anchors: others, options: ['REJECT_REPLACING', 'CANCEL']};
            }
            const forbidden = prior && prior.status === 'HUMAN_REJECTED' ? inState('FORBIDDEN') : [];
            const desired = uniq([...forbidden, ...edges]);
            return done(addEvents, reviewOf('HUMAN_REJECTED', desired), desired);
        }
        if (status === 'HUMAN_UNCERTAIN') {
            if (!prior || prior.status === 'HUMAN_UNCERTAIN' || choice === 'REGION_UNCERTAIN') {
                const base = prior && prior.status === 'HUMAN_UNCERTAIN' ? prior : {old_block_ids: [], new_block_ids: []};
                const review = {region_id: region.id, status: 'HUMAN_UNCERTAIN',
                    old_block_ids: [...new Set([...(base.old_block_ids || []), ...edges.map(e => e.old_block_id)])],
                    new_block_ids: [...new Set([...(base.new_block_ids || []), ...edges.map(e => e.new_block_id)])],
                    previous_review_id: prior ? prior.review_id || null : null};
                return {kind: 'EVENTS', linkEvents: addEvents, review,
                    plan: Core.planReview(region, reviews, edits, {status: 'HUMAN_UNCERTAIN', oldIds: review.old_block_ids,
                        newIds: review.new_block_ids, adds})};
            }
            const covered = inState('ANCHOR', 'FORBIDDEN').filter(s => !wanted.has(key(s)));
            if (!covered.length) return {kind: 'CHOICE', choice: 'UNCERTAIN_WHOLE_REGION', options: ['REGION_UNCERTAIN', 'CANCEL']};
            return done([], reviewOf(prior.status, covered), covered);
        }
        return error('INVALID_STATUS');
    }

    function launchLine(view) {
        if (!view || !view.capabilities || !view.capabilities.drafts_api) return '';
        const total = (view.prelinks || []).length;
        if (!total) return '';
        return TEXT.L1(total, (view.launch_summary || {}).stale_not_reconciled || 0);
    }

    return Object.freeze({CARDINALITY, STATE, STATES, VALIDITY, REASON, UNRESOLVED_REASON, STALE_REASON, ERROR, TEXT,
        cardinality, cardinalityLabel, edgesOf, composeLines, compilePromotion, launchLine});
}));
