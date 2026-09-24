(function (root) {
    'use strict';
    // Stage 2 «Смысловые блоки» — view only (phase B). One frozen V3 run, a sealed catalog
    // snapshot, or (before any analysis) the recognition blocks of the pair. Human Mapping rules
    // come from HumanMappingCore; every request of the workspace lives in this file and carries an
    // explicit scope. Nothing here writes: no POST/PUT, no storage but a per-tab session mark.
    // Russian texts are copied from UX_STATE_MATRIX (the only source of UI texts of the plan).

    const SIDES = ['OLD', 'NEW'];
    const VIEWER_SIDE = {OLD: 'left', NEW: 'right'};
    const TERMINAL = ['COMPLETED', 'FROZEN', 'COMPLETED_FROZEN'];
    const EDGE_LIMIT = 300;          // new, unmeasured threshold (UNIFIED_UX_DESIGN §5.2, C17)
    const MANY_PAGES = 6;
    const PAGE_CHUNK = 30;           // server limit of pages per page-blocks call
    // Same widths as the sheet viewer (app.js SC_CONTINUOUS_PREVIEW_WIDTH / SC_PREVIEW_WIDTH), so
    // the browser cache of page previews is shared with it (PERFORMANCE_PLAN §4.2).
    const STACK_PREVIEW_WIDTH = 1000;
    const PAGE_PREVIEW_WIDTH = 1400;
    const DEFAULT_RATIO = Math.SQRT2;  // width / height until the page geometry is known
    const DECIDED = ['ANCHOR', 'FORBIDDEN', 'NOT_COVERED_NEWER', 'UNCERTAIN'];
    const STATE_RANK = ['FORBIDDEN', 'ANCHOR', 'NOT_COVERED_NEWER', 'INVALID_TIMESTAMP', 'UNCERTAIN',
        'OUTSIDE_SELECTION', 'UNREVIEWED'];
    const enc = encodeURIComponent;

    // ── Texts (UX_STATE_MATRIX §1, §3, §4 — verbatim with real values) ──────────
    function plural(n, one, few, many) {
        const a = Math.abs(Number(n) || 0) % 100, b = a % 10;
        if (a > 10 && a < 20) return many;
        if (b > 1 && b < 5) return few;
        return b === 1 ? one : many;
    }
    const T = {
        A: n => `Анализ нашёл на этих листах смысловые регионы: ${n}. Связей блоков пока нет — выделите блок слева и справа и соедините их.`,
        A_NN: id => `В регионе ${id} (N↔N) ИИ не предложил пар блоков. Выделите блок слева и справа и соедините их.`,
        B_NEWER: n => `Связей, созданных после решения региона и ещё не закреплённых: ${n}. Повторите решение, чтобы включить их.`,
        B_CURRENT: (what, tail) => `Текущее решение: ${what}${tail}. Новое решение заменит его целиком; прежнее останется в журнале.`,
        E_MANY: 'Показаны видимые страницы; прокрутите для остальных.',
        F: 'Смысловые регионы строит анализ изменений (шаг 3). Сейчас можно посмотреть блоки распознавания и проверить, правильно ли сопоставлены листы. Связи блоков сохраняются в конкретный результат анализа и станут доступны после первого анализа.',
        F_SUMMARY: (o, n, hidden) => `Блоки распознавания: OLD ${o} · NEW ${n}` + (hidden ? ' (штампы скрыты)' : ''),
        F1: (when, reason) => `Последний запуск анализа ${when} не завершился: ${reason}.`,
        F1_REASON: {v3_inference_kill_switch: 'ИИ-анализ отключён администратором',
            v3_provider_unavailable: 'модель недоступна или исчерпан лимит подписки'},
        F1_OTHER: 'см. подробности',
        F2: (when, run, date) => `Последний запуск анализа ${when} не завершился. Показан предыдущий результат ${run} от ${date}.`,
        G_LIVE: (run, total, reviews, events) => `В истории прогона ${run} записей: ${total} (решений ${reviews}, событий связей ${events}). Новое решение региона заменит текущее; прежние останутся в журнале.`,
        JOURNAL_HEAD: 'Автор в данных не хранится.',
        OUTSIDE_RESULT: 'записано вне этого результата',
        OUTSIDE_SNAPSHOT: 'записано вне этого снимка',
        G_SNAP: 'Решения этого снимка хранятся отдельно от результатов анализа и не могут стать якорями нового анализа.',
        G_SNAP_FOOTER: s => `Снимок ${s} — только просмотр.`,
        H: n => `Решения по блокам не меняют полученный результат анализа (${n} ${plural(n, 'изменение', 'изменения', 'изменений')}).`,
        FOOTER: run => `Решения записываются в историю прогона ${run}. Результат «Изменения проекта» этого прогона заморожен и не меняется. Подтверждённые связи станут якорями только при новом анализе с их учётом (функция в разработке).`,
        I_RUN: 'Пары листов не построены — смысловые блоки открываются по регионам. Постройте карту листов, чтобы видеть регионы по парам листов.',
        I_NORUN: 'Пары листов не построены. Нажмите «Обработать», чтобы построить карту листов.',
        J_RUN: run => `Идёт новый анализ. Решения записываются в прогон ${run}; новый прогон начнёт с пустой истории связей.`,
        J_NORUN: 'Идёт анализ изменений. Смысловые регионы появятся после его завершения.',
        K0: 'Файлы распознавания перезаписаны без изменений — работа с блоками доступна.',
        K_PDF: run => `Документ пары изменился после анализа ${run}. Решения нельзя будет использовать — перезапустите анализ.`,
        K_BLOCKS: run => `Распознавание пары изменилось после анализа ${run}. Новые решения могли бы указывать на другие блоки, поэтому запись закрыта.`,
        L: 'Редактирование смысловых связей в этом разделе ещё не включено. Для правки откройте Human Mapping ↗',
        M1: n => `История прогона содержит событий, которые проверка для нового анализа отвергнет: ${n}. История только дополняется — исправить можно только в следующем прогоне.`,
        M2: (a, b) => `Одна и та же связь закреплена в ${a} и запрещена в ${b} — снимок для нового анализа не соберётся.`,
        N: (next, run) => `Появился новый результат анализа ${next} (текущий). Решения по блокам относятся к ${run} и в новый результат не переносятся.`,
        O: 'Пара листов не подтверждена — блоки показаны по предложению.',
        P: label => `У листа ${label} нет пары. Показана одна сторона; чтобы соединить блоки, включите «Весь регион».`,
        FAIL1: run => `Данные смысловых блоков прогона ${run} недоступны. Журнал решений показан как есть (идентификаторы блоков текстом).`,
        FAIL2: page => `Нет распознавания страницы ${page} (blocks/Markdown) — блоки недоступны.`,
        FAIL3: page => `Страница ${page} недоступна в PDF пары.`,
        FAIL4: run => `Результат ${run} недоступен или изменён вне системы. Решения в его истории не удалены.`,
        FAIL5: run => `Данные смысловых блоков прогона ${run} не принадлежат выбранному результату.`,
        FAIL6: (n, run, parts) => `Связь листов сохранена. ${n} ${plural(n, 'решение', 'решения', 'решений')} по смысловым блокам ${plural(n, 'осталось', 'остались', 'остались')} в силе (прогон ${run}); теперь они показаны: ${parts.join(', ')}.`,
        FAIL6_IN_ROW: (k, left, right) => `${k} — в паре ${left} ↔ ${right}`,
        FAIL6_CROSS: k => `${k} — концы в разных парах листов`,
        FAIL6_OFF: k => `${k} — вне карты листов`,
        ROW_CHANGED: (before, after) => `Пара листов изменена: было ${before} · стало ${after}`,
        FAIL11: n => `Блоки снимка не совпадают с распознаванием этой версии (${n}). Показана геометрия снимка.`,
        FAIL14: 'Не удалось загрузить смысловые блоки.',
        MARK: {UNKNOWN_OR_WRONG_SIDE_BLOCK: 'блок не найден в данных результата', BLOCK_OUTSIDE_REGION: 'вне региона',
            STALE_REGION: 'регион не найден'},
        CTX_MARK: '⟳ контекст изменился',
        UNTOUCHED: n => `◇ Смысловые регионы вне сопоставленных пар листов: ${n}`,
        UNPAIRED: n => `Без общей пары листов (${n})`,
        UNPAIRED_HINT: 'Нет пары листов, где видны обе стороны региона',
        SUMMARY: (who, a, f, w) => `Смысловые связи · ${who}: закреплено ${a} · запрещено ${f} · требуют внимания ${w}`,
        CHIP_TITLE: (r, a, f, w) => `Смысловые регионы анализа на этих листах: ${r}. Закреплено связей: ${a}, запрещено: ${f}, требуют внимания: ${w}.`,
        SHEETS_DONE: 'Все листы решены',
        BRIDGE_OK: (a, r) => `Снимок для нового анализа собирается: якорей ${a} · запретов ${r}. Это предварительная проверка; якорный анализ пока недоступен.`,
        BRIDGE_EMPTY: 'Снимок собирается, но ограничений нет: ни одна связь не закреплена и не запрещена.',
        BRIDGE: {
            CONFIRMED_AND_REJECTED_EXACT_EDGE: 'Не собирается: одна и та же связь закреплена в одном регионе и запрещена в другом.',
            BLOCK_OUTSIDE_REGION: 'Не собирается: в истории есть записи с блоками вне региона или с неизвестным регионом (см. журнал).',
            UNKNOWN_OR_WRONG_SIDE_BLOCK: 'Не собирается: в истории есть записи с блоками вне региона или с неизвестным регионом (см. журнал).',
            STALE_REGION: 'Не собирается: в истории есть записи с блоками вне региона или с неизвестным регионом (см. журнал).',
            STALE_SOURCE_PDF: 'Не собирается: документ пары изменился после анализа.',
            HUMAN_MAPPING_SOURCE_CHANGED: 'Не собирается: данные смысловых блоков не совпадают с замороженным результатом анализа.',
            HUMAN_MAPPING_SCOPE_MISMATCH: 'Не собирается: данные смысловых блоков не совпадают с замороженным результатом анализа.',
            HUMAN_EVENT_SCOPE_MISMATCH: 'Не собирается: в истории есть записи другого результата или другой пары.',
            HUMAN_HISTORY_CHANGED_DURING_READ: 'История менялась во время проверки. Повторите проверку.',
        },
        BRIDGE_OTHER: code => `Не собирается: ${code}. Подробности — в журнале.`,
    };
    const MODALITY = {TEXT: 'текст', TABLE: 'таблица', GRAPHIC: 'графика'};
    const STATE_LABEL = {UNREVIEWED: 'не проверена', UNCERTAIN: 'не уверен', OUTSIDE_SELECTION: 'вне решения региона',
        NOT_COVERED_NEWER: 'создана после решения', ANCHOR: 'закреплена (якорь)', FORBIDDEN: 'запрещена именно эта связь',
        CROSS_REGION_CONFLICT: 'конфликт между регионами', INVALID_TIMESTAMP: 'неверная метка времени'};
    const GLYPH = {UNCERTAIN: '?', NOT_COVERED_NEWER: '◌', ANCHOR: '✓', FORBIDDEN: '⊘', CROSS_REGION_CONFLICT: '⚠',
        INVALID_TIMESTAMP: '!'};
    const REVIEW_LABEL = {HUMAN_CONFIRMED: '✓ Подтверждено', HUMAN_REJECTED: '⊘ Отклонено', HUMAN_UNCERTAIN: '? Не уверен'};
    const EVENT_LABEL = {ADD_BLOCK_LINK: 'связь создана', DELETE_BLOCK_LINK: 'связь удалена',
        REASSIGN_BLOCK_LINK: 'связь переназначена'};
    const CARDINALITY = {'1:1': '1→1', '1:N': '1→N', 'N:1': 'N→1', 'N:N': 'N↔N', EMPTY: 'пустая сторона'};

    // ── Formatting ────────────────────────────────────────────────────────────
    const pad2 = n => String(n).padStart(2, '0');
    function parseDate(value) {
        if (!value) return null;
        const d = new Date(value);
        return Number.isNaN(d.getTime()) ? null : d;
    }
    function dayMonth(value) {
        const d = parseDate(value);
        return d ? `${pad2(d.getDate())}.${pad2(d.getMonth() + 1)}` : '—';
    }
    function clock(value) {
        const d = parseDate(value);
        return d ? `${pad2(d.getHours())}:${pad2(d.getMinutes())}` : '—';
    }
    const dayMonthTime = value => (parseDate(value) ? `${dayMonth(value)} ${clock(value)}` : '—');
    const short = id => String(id || '').slice(0, 8);
    const snapShort = id => String(id || '').slice(0, 13) + '…';
    const modality = m => MODALITY[m] || String(m || '').toLowerCase() || 'блок';
    const clip = (text, n) => (String(text || '').length > n ? String(text).slice(0, n - 1).trimEnd() + '…' : String(text || ''));
    const uniqSorted = list => [...new Set((list || []).map(Number).filter(Boolean))].sort((a, b) => a - b);
    const joinPages = list => uniqSorted(list).join(', ');

    // ── API client (explicit, never patches global fetch) ─────────────────────
    class ApiError extends Error {
        constructor(status, code, body) {
            super(code || ('HTTP ' + status));
            this.status = status;
            this.code = code;
            this.body = body;
        }
    }
    // Three HM body forms (C10): {detail:{error}}, {detail:"text"}, legacy {error}.
    function errorCode(body) {
        if (!body || typeof body !== 'object') return '';
        const detail = body.detail;
        if (detail && typeof detail === 'object' && detail.error) return String(detail.error);
        if (typeof detail === 'string') return detail;
        return body.error ? String(body.error) : '';
    }
    const bmBase = (sid, pid) => `/api/stage-comparison/sessions/${enc(sid)}/pairs/${enc(pid)}/block-mapping`;
    const hmBase = (oid, pid) => `/api/human-mapping/objects/${enc(oid)}/comparisons/${enc(pid)}`;
    const scopeQuery = scope => (scope.resultId ? `result_id=${enc(scope.resultId)}`
        : `session_id=${enc(scope.sessionId)}&run_id=${enc(scope.runId)}`);
    // Fallback only: a separate cache key when the viewer's PDF signature is not known yet.
    function previewUrl(sid, pid, side, page, pdfSha, width = STACK_PREVIEW_WIDTH) {
        return `/api/stage-comparison/sessions/${enc(sid)}/pairs/${enc(pid)}/page-preview?side=${VIEWER_SIDE[side]}`
            + `&page=${Number(page)}&width=${Number(width)}&v=${enc(String(pdfSha || '').slice(0, 12))}`;
    }
    function hmAssetUrl(oid, pid, sid, run, side, page) {
        return `${hmBase(oid, pid)}/assets/${side.toLowerCase()}/p${String(Number(page)).padStart(3, '0')}/full_page.png`
            + `?session_id=${enc(sid)}&run_id=${enc(run)}`;
    }
    function humanMappingHref(oid, pid, sid, run) {
        return `/human-mapping/?object=${enc(oid)}&comparison=${enc(pid)}&session_id=${enc(sid)}&run_id=${enc(run)}`;
    }
    function createClient(fetchImpl) {
        async function get(url, signal) {
            const response = await fetchImpl(url, signal ? {signal} : undefined);
            let body = null;
            try { body = await response.json(); } catch (_) { body = null; }
            if (!response.ok) throw new ApiError(response.status, errorCode(body), body);
            return body;
        }
        const pageList = pages => pages.map(Number).join(',');
        return {
            status: (sid, pid) => get(bmBase(sid, pid) + '/status'),
            regionIndex: (sid, pid, run) => get(`${bmBase(sid, pid)}/runs/${enc(run)}/region-index`),
            runPageBlocks: (sid, pid, run, side, pages, signal) =>
                get(`${bmBase(sid, pid)}/runs/${enc(run)}/page-blocks?side=${side}&pages=${pageList(pages)}`, signal),
            sourcePageBlocks: (sid, pid, side, pages, signal) =>
                get(`${bmBase(sid, pid)}/source/page-blocks?side=${side}&pages=${pageList(pages)}`, signal),
            runBlock: (sid, pid, run, side, id, page) =>
                get(`${bmBase(sid, pid)}/runs/${enc(run)}/blocks/${side}/${enc(id)}` + (page ? `?page=${Number(page)}` : '')),
            sourceBlock: (sid, pid, side, id) => get(`${bmBase(sid, pid)}/source/blocks/${side}/${enc(id)}`),
            bridgeCheck: (sid, pid, run) => get(`${bmBase(sid, pid)}/runs/${enc(run)}/bridge-check`),
            hmReviews: (oid, pid, scope) => get(`${hmBase(oid, pid)}/reviews?${scopeQuery(scope)}`),
            hmBlockLinks: (oid, pid, scope) => get(`${hmBase(oid, pid)}/block-links?${scopeQuery(scope)}`),
            hmUiData: (oid, pid, scope) => get(`${hmBase(oid, pid)}/ui-data?${scopeQuery(scope)}`),
        };
    }

    // ── Binding order (D-6, DATA_CONTRACT_PLAN §2.2, C13) ─────────────────────
    function chooseBinding(status, focus, pairId) {
        if (focus && focus.pair_id === pairId) {
            if (focus.result_source === 'SEALED_SNAPSHOT' && focus.result_id) {
                return {binding: {kind: 'SNAP', resultId: String(focus.result_id)}, source: 'catalog'};
            }
            // A sealed snapshot's source_run_id is the run it was cut from, not a run of this pair.
            if (focus.source_run_id && focus.result_source !== 'SEALED_SNAPSHOT') {
                return {binding: {kind: 'LIVE', runId: String(focus.source_run_id)}, source: 'catalog'};
            }
        }
        if (status && status.current_run_id) {
            return {binding: {kind: 'LIVE', runId: String(status.current_run_id)}, source: 'current'};
        }
        return {binding: null, source: null};
    }
    const bindingKey = (b, sid) => (!b ? '' : b.kind === 'SNAP' ? 'SNAP:' + b.resultId : `LIVE:${sid}:${b.runId}`);

    // ── Regions: server index or snapshot ui-data → HumanMappingCore shape ────
    function cardinalityOf(olds, news) {
        if (!olds || !news) return 'EMPTY';
        if (olds === 1 && news === 1) return '1:1';
        if (olds === 1) return '1:N';
        return news === 1 ? 'N:1' : 'N:N';
    }
    function fromIndexRegion(r) {
        const members = r.members || {}, allowed = r.allowed || {}, ms = r.membership_state || {};
        const pages = {};
        for (const side of SIDES) {
            // allowed comes from the server (validation.allowed_block_ids); for an explicitly EMPTY side
            // the page context sits on the first page so the core's allowedBlockIds sees the same ids.
            pages[side] = ((r.pages || {})[side] || []).map((page, i) => ({page: Number(page),
                blocks: ms[side] === 'EMPTY' && i === 0 ? (allowed[side] || []).map(id => ({id})) : []}));
        }
        const block = b => ({id: b.id, page: Number(b.page), type: b.type});
        const olds = (members.OLD || []).map(block), news = (members.NEW || []).map(block);
        return {id: r.id, domain: String(r.domain || r.title || ''), scope: String(r.scope || ''),
            old_blocks: olds, new_blocks: news, membership_state: ms, mapping_state: r.mapping_state || 'MAPPED',
            member_cardinality: r.member_cardinality || cardinalityOf(olds.length, news.length), pages,
            allowed: {OLD: new Set(allowed.OLD || []), NEW: new Set(allowed.NEW || [])}};
    }
    function fromUiRegion(r, Core) {
        const olds = r.old_blocks || [], news = r.new_blocks || [];
        return {...r, domain: String(r.domain || ''), scope: String(r.scope || ''),
            member_cardinality: cardinalityOf(olds.length, news.length),
            allowed: {OLD: new Set(Core.allowedBlockIds(r, 'OLD')), NEW: new Set(Core.allowedBlockIds(r, 'NEW'))}};
    }

    // Everything derived from one binding: edge states (same order of checks as the bridge),
    // decisions, conflicts and the client-side history recheck (display only, C29).
    function buildModel({regions, reviews, edits, scope}) {
        const Core = root.HumanMappingCore;
        reviews = reviews || [];
        edits = edits || [];
        const byId = new Map(regions.map(r => [r.id, r]));
        const latest = new Map(), info = new Map(), edges = new Map(), blockRegions = new Map(), blockType = new Map();
        const linkEnds = new Set(), reviewSel = new Set(), decided = [];
        for (const region of regions) {
            for (const side of SIDES) {
                for (const id of region.allowed[side]) {
                    const key = side + '|' + id;
                    if (!blockRegions.has(key)) blockRegions.set(key, []);
                    if (!blockRegions.get(key).includes(region.id)) blockRegions.get(key).push(region.id);
                }
                for (const b of (side === 'OLD' ? region.old_blocks : region.new_blocks) || []) {
                    if (b.type && !blockType.has(side + '|' + b.id)) blockType.set(side + '|' + b.id, {type: b.type, page: b.page});
                }
            }
            const review = Core.latestReview(region, reviews);
            latest.set(region.id, review);
            if (review) {
                for (const id of review.old_block_ids || []) reviewSel.add('OLD|' + id);
                for (const id of review.new_block_ids || []) reviewSel.add('NEW|' + id);
            }
            let anchors = 0, forbidden = 0, newer = 0;
            const rows = Core.deriveEdgeStates(region, reviews, edits);
            for (const row of rows) {
                const key = Core.edgeKey(row.old_block_id, row.new_block_id);
                const oldPage = Core.blockPage(region, 'OLD', row.old_block_id);
                const newPage = Core.blockPage(region, 'NEW', row.new_block_id);
                linkEnds.add('OLD|' + row.old_block_id);
                linkEnds.add('NEW|' + row.new_block_id);
                if (!edges.has(key)) {
                    edges.set(key, {key, old_block_id: row.old_block_id, new_block_id: row.new_block_id,
                        oldPage: null, newPage: null, regionIds: [], links: []});
                }
                const edge = edges.get(key);
                if (edge.oldPage == null && oldPage != null) edge.oldPage = Number(oldPage);
                if (edge.newPage == null && newPage != null) edge.newPage = Number(newPage);
                edge.regionIds.push(region.id);
                edge.links.push({region_id: region.id, link_id: row.link_id, source: row.source,
                    membership_kind: row.membership_kind, state: row.state});
                if (row.state === 'ANCHOR') anchors++;
                else if (row.state === 'FORBIDDEN') forbidden++;
                else if (row.state === 'NOT_COVERED_NEWER') newer++;
                if (review && DECIDED.includes(row.state) && oldPage != null && newPage != null) {
                    decided.push({region_id: region.id, link_id: row.link_id, key, oldPage: Number(oldPage),
                        newPage: Number(newPage)});
                }
            }
            info.set(region.id, {status: review ? review.status : 'UNREVIEWED', review, links: rows.length,
                anchors, forbidden, newer,
                zeroConstraint: !!review && review.status !== 'HUMAN_UNCERTAIN' && !anchors && !forbidden});
        }
        for (const edge of edges.values()) {
            const states = edge.links.map(l => l.state);
            edge.conflict = states.includes('ANCHOR') && states.includes('FORBIDDEN');
            edge.state = edge.conflict ? 'CROSS_REGION_CONFLICT' : (STATE_RANK.find(s => states.includes(s)) || 'UNREVIEWED');
        }
        const issues = Core.historyIssues({regions, reviews, edits, scope: scope || null});
        const issueById = new Map();
        for (const issue of issues) if (issue.id && issue.code !== 'CONFIRMED_AND_REJECTED_EXACT_EDGE') issueById.set(issue.id, issue.code);
        const issueRegions = new Set([...reviews, ...edits]
            .filter(e => issueById.has(e.review_id || e.event_id)).map(e => e.region_id));
        const conflicts = [...edges.values()].filter(e => e.conflict);
        for (const [id, i] of info) {
            i.conflict = conflicts.some(c => c.regionIds.includes(id));
            i.attention = i.newer > 0 || i.zeroConstraint || i.conflict || issueRegions.has(id);
        }
        return {regions, byId, latest, info, edges, blockRegions, blockType, linkEnds, reviewSel, decided,
            issues, issueById, eventIssues: issueById.size, conflicts, reviews, edits};
    }

    function edgeTouchesRow(edge, row) {
        return (edge.oldPage != null && (row.leftPages || []).map(Number).includes(edge.oldPage))
            || (edge.newPage != null && (row.rightPages || []).map(Number).includes(edge.newPage));
    }
    function countsFor(model, regionList, edgeList) {
        const anchors = edgeList.filter(e => e.links.some(l => l.state === 'ANCHOR')).length;
        const forbidden = edgeList.filter(e => e.links.some(l => l.state === 'FORBIDDEN')).length;
        const attentionEdges = edgeList.filter(e => e.conflict || e.links.some(l => ['NOT_COVERED_NEWER', 'INVALID_TIMESTAMP'].includes(l.state))).length;
        const zero = regionList.filter(r => (model.info.get(r.id) || {}).zeroConstraint).length;
        return {anchors, forbidden, attention: attentionEdges + zero};
    }
    function rowChipOf(model, row) {
        const Core = root.HumanMappingCore;
        if (!model || !row) return null;
        const regions = model.regions.filter(r => Core.rel(r, row) !== 'NONE');
        const edges = [...model.edges.values()].filter(e => edgeTouchesRow(e, row));
        const c = countsFor(model, regions, edges);
        if (!regions.length && !c.anchors && !c.forbidden && !c.attention) return null;
        const label = ['◇' + regions.length, c.anchors && '✓' + c.anchors, c.forbidden && '⊘' + c.forbidden,
            c.attention && '⚠' + c.attention].filter(Boolean).join(' ');
        return {label, regions: regions.length, anchors: c.anchors, forbidden: c.forbidden, attention: c.attention,
            title: T.CHIP_TITLE(regions.length, c.anchors, c.forbidden, c.attention)};
    }
    function summaryOf(model) {
        const c = countsFor(model, model.regions, [...model.edges.values()]);
        return {...c, attention: c.attention + model.eventIssues};
    }

    // Placement notice (MASTER §12, STALE_DECISION_POLICY §3.3 step 4): only the saved links count.
    function pgkLabel(pgk) {
        const m = /^L([^|]*)\|R(.*)$/.exec(pgk || '');
        return m ? [m[1].split(',').join(', '), m[2].split(',').join(', ')] : ['', ''];
    }
    function placementNotice(decided, before, after, runLabel) {
        const Core = root.HumanMappingCore;
        const changed = decided.filter(d => Core.placementChanged(Core.placementClass(d.oldPage, d.newPage, before),
            Core.placementClass(d.oldPage, d.newPage, after)));
        if (!changed.length) return null;
        const inRow = new Map();
        let cross = 0, off = 0;
        for (const d of changed) {
            const cls = Core.placementClass(d.oldPage, d.newPage, after);
            if (cls.kind === 'IN_ROW') {
                const k = cls.rows.join(';');
                inRow.set(k, (inRow.get(k) || 0) + 1);
            } else if (cls.kind === 'CROSS_ROWS') cross++;
            else off++;
        }
        const parts = [...inRow.entries()].map(([k, n]) => {
            const [l, r] = pgkLabel(k.split(';')[0]);
            return T.FAIL6_IN_ROW(n, l, r);
        });
        if (cross) parts.push(T.FAIL6_CROSS(cross));
        if (off) parts.push(T.FAIL6_OFF(off));
        return {text: T.FAIL6(changed.length, runLabel, parts), changed};
    }

    // Stamps are hidden unless shown, except a stamp that is a link end or in a review selection (C26).
    function isStamp(block) { return block.source_block_type === 'stamp'; }
    function visibleBlocks(side, blocks, {showStamps, linkEnds, reviewSel}) {
        return (blocks || []).filter(b => !isStamp(b) || showStamps
            || (linkEnds && linkEnds.has(side + '|' + b.block_id)) || (reviewSel && reviewSel.has(side + '|' + b.block_id)));
    }

    // Link geometry in canvas coordinates. centers: {OLD: Map id→{x,y}, NEW: …} of rendered blocks;
    // rects: panel viewports {left, top, width, height} (null when the panel is absent).
    // Each segment is clipped per panel (HumanMappingCore.clipSegment); an end that is off the
    // lens or scrolled away becomes a port marker on the panel edge.
    function computeLinkGeometry({edges, centers, rects}) {
        const Core = root.HumanMappingCore;
        const inside = (p, r) => !!r && p.x >= r.left && p.x <= r.left + r.width && p.y >= r.top && p.y <= r.top + r.height;
        const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
        const onEdge = (p, r) => ({x: clamp(p.x, r.left, r.left + r.width), y: clamp(p.y, r.top + 6, r.top + r.height - 6)});
        const out = [];
        for (const edge of edges || []) {
            const a = centers.OLD.get(edge.old_block_id) || null, b = centers.NEW.get(edge.new_block_id) || null;
            if (!a && !b) continue;
            const ports = [];
            let start, end;
            const fit = (p, other, rect, which) => {
                if (inside(p, rect)) return p;
                const seg = rect && Core.clipSegment(which === 'a' ? p : other, which === 'a' ? other : p, rect);
                const q = seg ? (which === 'a' ? seg.a : seg.b) : onEdge(p, rect);
                ports.push({side: which === 'a' ? 'OLD' : 'NEW', x: q.x, y: q.y, page: which === 'a' ? edge.oldPage : edge.newPage,
                    kind: 'scrolled'});
                return q;
            };
            if (a && b && rects.OLD && rects.NEW) {
                start = fit(a, b, rects.OLD, 'a');
                end = fit(b, a, rects.NEW, 'b');
            } else if (a) {
                const own = rects.OLD;
                if (!own) continue;
                start = inside(a, own) ? a : onEdge(a, own);
                if (start !== a) ports.push({side: 'OLD', x: start.x, y: start.y, page: edge.oldPage, kind: 'scrolled'});
                const target = rects.NEW || null;
                end = target ? {x: target.left, y: clamp(start.y, target.top + 6, target.top + target.height - 6)}
                    : {x: own.left + own.width, y: start.y};
                ports.push({side: 'NEW', x: end.x, y: end.y, page: edge.newPage, kind: 'offlens', single: !target});
            } else {
                const own = rects.NEW;
                if (!own) continue;
                end = inside(b, own) ? b : onEdge(b, own);
                if (end !== b) ports.push({side: 'NEW', x: end.x, y: end.y, page: edge.newPage, kind: 'scrolled'});
                const target = rects.OLD || null;
                start = target ? {x: target.left + target.width, y: clamp(end.y, target.top + 6, target.top + target.height - 6)}
                    : {x: own.left, y: end.y};
                ports.push({side: 'OLD', x: start.x, y: start.y, page: edge.oldPage, kind: 'offlens', single: !target});
            }
            // The hit path skips both ends so a click on a block centre still reaches the block.
            const at = t => ({x: start.x + (end.x - start.x) * t, y: start.y + (end.y - start.y) * t});
            out.push({key: edge.key, d: Core.linkPath(start, end), hit: Core.linkPath(at(0.15), at(0.85)), start, end,
                mid: at(0.5), ports});
        }
        return out;
    }

    function parseTable(t) {
        if (Array.isArray(t)) return t.map(row => (Array.isArray(row) ? row.map(String) : [String(row)]));
        const rows = String(t || '').split('\n').map(l => l.trim())
            .filter(l => l.startsWith('|') && !/^\|[\s:|-]+\|?$/.test(l))
            .map(l => l.replace(/^\||\|$/g, '').split('|').map(c => c.trim()));
        return rows.length ? rows : null;
    }

    function safeSessionStorage() {
        try { return root.sessionStorage || null; } catch (_) { return null; }
    }

    // ── Store ─────────────────────────────────────────────────────────────────
    function emptyState() {
        return {
            sessionId: '', pairId: '', loading: false,
            status: null, statusError: null, catalogFocus: null,
            binding: null, bindingSource: null, boundCurrent: null, dismissedCurrent: null,
            index: null, uiData: null, reviews: null, edits: null,
            dataError: null, historyError: null, snapRequested: false,
            blocksActive: false,
            pages: {OLD: {}, NEW: {}}, geometry: {OLD: {}, NEW: {}}, pageError: null,
            details: {},
            lens: {key: '', row: null, mode: 'pair', focus: '', extra: {OLD: [], NEW: []}},
            selection: {OLD: [], NEW: [], link: '', block: null},
            showStamps: false, focusOnly: false, showAllEdges: false,
            notice: null, rowNotice: null, ctxMarks: {},
            bridge: {status: 'idle', text: '', details: '', checkedAt: '', cached: false, conflict: false},
            version: 0,
        };
    }

    function createStore(options = {}) {
        if (!root.HumanMappingCore) return null;
        const Core = root.HumanMappingCore;
        const Vue = options.Vue !== undefined ? options.Vue : root.Vue;
        const reactive = Vue && Vue.reactive ? Vue.reactive : (x => x);
        const markRaw = Vue && Vue.markRaw ? Vue.markRaw : (x => x);
        const client = createClient(options.fetch || ((url, init) => root.fetch(url, init)));
        const storage = options.sessionStorage !== undefined ? options.sessionStorage : safeSessionStorage();
        const S = reactive(emptyState());
        let token = 0, bindToken = 0;
        let controllers = [];
        let linksBaseline = null, baselinePair = '';
        let modelCache = {version: -1, model: null};
        let previewBuilder = null;
        let previewCache = new Map();
        const chipCache = new Map();
        let chipVersion = -1;

        const bump = () => { S.version++; };
        const raw = value => (value && typeof value === 'object' ? markRaw(value) : value);
        function abortPages() {
            for (const c of controllers) { try { c.abort(); } catch (_) { /* already settled */ } }
            controllers = [];
        }
        const statusRuns = () => (S.status && Array.isArray(S.status.runs) ? S.status.runs : []);
        const run = () => (S.binding && S.binding.kind === 'LIVE' ? statusRuns().find(r => r.run_id === S.binding.runId) || null : null);
        const snapshot = () => (S.binding && S.binding.kind === 'SNAP'
            ? ((S.status && S.status.snapshots) || []).find(s => s.result_id === S.binding.resultId) || null : null);
        const objectId = () => (S.status && S.status.object_id) || '';
        const bindingLabel = () => (!S.binding ? '' : S.binding.kind === 'SNAP' ? snapShort(S.binding.resultId) : short(S.binding.runId));
        const runUsable = r => !!r && r.write_block_reason !== 'RUN_INVALID';

        function resetBindingData() {
            abortPages();
            Object.assign(S, {index: null, uiData: null, reviews: null, edits: null, dataError: null, historyError: null,
                snapRequested: false, pages: {OLD: {}, NEW: {}}, geometry: {OLD: {}, NEW: {}}, pageError: null, details: {},
                selection: {OLD: [], NEW: [], link: '', block: null},
                bridge: {status: 'idle', text: '', details: '', checkedAt: '', cached: false, conflict: false}});
            S.lens = {...S.lens, focus: '', extra: {OLD: [], NEW: []}};
            bump();
        }

        async function load({sessionId = '', pairId = '', catalogFocus = null} = {}) {
            const my = ++token;
            bindToken++;
            abortPages();
            const keep = {showStamps: false};
            Object.assign(S, emptyState(), keep);
            S.sessionId = sessionId;
            S.pairId = pairId;
            S.catalogFocus = catalogFocus || null;
            linksBaseline = null;
            baselinePair = '';
            previewCache = new Map();
            bump();
            if (!sessionId || !pairId) return;
            S.loading = true;
            try {
                const status = await client.status(sessionId, pairId);
                if (my !== token) return;
                S.status = raw(status);
            } catch (error) {
                if (my !== token) return;
                S.statusError = {code: error.code || '', status: error.status || 0};
                S.loading = false;
                bump();
                return;
            }
            const choice = chooseBinding(S.status, S.catalogFocus, pairId);
            await applyBinding(choice.binding, choice.source);
            if (my === token) S.loading = false;
        }

        async function applyBinding(binding, source) {
            const my = ++bindToken;
            S.binding = binding;
            S.bindingSource = source;
            S.boundCurrent = S.status ? S.status.current_run_id || null : null;
            resetBindingData();
            if (!binding) {
                if (S.blocksActive) ensureLensPages();
                return;
            }
            if (binding.kind === 'SNAP') {
                if (S.blocksActive) await loadSnapshot();
                return;
            }
            const r = run();
            const oid = objectId();
            if (!r) {
                S.dataError = {kind: 'FAIL4'};
                bump();
                return;
            }
            const scope = {sessionId: S.sessionId, runId: binding.runId};
            const wantIndex = r.hm_available && runUsable(r);
            const wantHistory = !!oid && runUsable(r);
            const [index, reviews, edits] = await Promise.allSettled([
                wantIndex ? client.regionIndex(S.sessionId, S.pairId, binding.runId) : Promise.resolve(null),
                wantHistory ? client.hmReviews(oid, S.pairId, scope) : Promise.resolve(null),
                wantHistory ? client.hmBlockLinks(oid, S.pairId, scope) : Promise.resolve(null),
            ]);
            if (my !== bindToken) return;
            if (index.status === 'fulfilled') S.index = raw(index.value);
            else S.dataError = indexFailure(index.reason);
            if (!r.hm_available && !S.dataError) S.dataError = {kind: hmFailureKind(r.hm_reason)};
            if (!runUsable(r)) S.dataError = {kind: 'FAIL4'};
            if (reviews.status === 'fulfilled' && edits.status === 'fulfilled') {
                S.reviews = raw(Array.isArray(reviews.value) ? reviews.value : []);
                S.edits = raw(Array.isArray(edits.value) ? edits.value : []);
            } else if (wantHistory) {
                const reason = reviews.status === 'rejected' ? reviews.reason : edits.reason;
                S.historyError = reason && reason.code === 'Human Mapping result unavailable' ? {kind: 'FAIL4'} : {kind: 'FAIL14'};
            }
            bump();
            if (S.blocksActive) ensureLensPages();
        }
        function hmFailureKind(reason) {
            return ['HM_SCOPE_MISMATCH', 'HM_SCHEMA_MISMATCH'].includes(reason) ? 'FAIL5' : 'FAIL1';
        }
        function indexFailure(error) {
            if (!error) return {kind: 'FAIL14'};
            if (error.code === 'RUN_INVALID' || error.code === 'RUN_NOT_FOUND') return {kind: 'FAIL4'};
            if (error.code === 'HM_UNAVAILABLE') return {kind: hmFailureKind(((error.body || {}).detail || {}).hm_reason)};
            return {kind: 'FAIL14'};
        }

        async function loadSnapshot() {
            if (!S.binding || S.binding.kind !== 'SNAP' || S.snapRequested) return;
            const my = bindToken;
            S.snapRequested = true;
            const oid = objectId();
            if (!oid) {
                S.dataError = {kind: 'FAIL1'};
                bump();
                return;
            }
            const scope = {resultId: S.binding.resultId};
            const [ui, reviews, edits] = await Promise.allSettled([client.hmUiData(oid, S.pairId, scope),
                client.hmReviews(oid, S.pairId, scope), client.hmBlockLinks(oid, S.pairId, scope)]);
            if (my !== bindToken) return;
            if (ui.status === 'fulfilled' && ui.value && Array.isArray(ui.value.regions)) S.uiData = raw(ui.value);
            else S.dataError = ui.status === 'rejected' && ui.reason.code === 'HUMAN_MAPPING_UI_DATA_NOT_FOUND'
                ? {kind: 'FAIL1'} : {kind: 'FAIL14'};
            if (reviews.status === 'fulfilled' && edits.status === 'fulfilled') {
                S.reviews = raw(Array.isArray(reviews.value) ? reviews.value : []);
                S.edits = raw(Array.isArray(edits.value) ? edits.value : []);
            } else S.historyError = {kind: 'FAIL14'};
            bump();
            ensureLensPages();
        }

        function retry() {
            if (S.statusError || !S.status) return load({sessionId: S.sessionId, pairId: S.pairId, catalogFocus: S.catalogFocus});
            S.pages = {OLD: {}, NEW: {}};
            S.pageError = null;
            return applyBinding(S.binding, S.bindingSource);
        }
        async function refreshStatus() {
            if (!S.sessionId || !S.pairId || !S.status) return;
            const my = token;
            try {
                const status = await client.status(S.sessionId, S.pairId);
                if (my === token) { S.status = raw(status); bump(); }
            } catch (_) { /* the banner check is best effort */ }
        }
        function setCatalogFocus(focus) {
            S.catalogFocus = focus || null;
            if (!S.status || S.bindingSource === 'explicit') return;
            const choice = chooseBinding(S.status, S.catalogFocus, S.pairId);
            if (bindingKey(choice.binding, S.sessionId) !== bindingKey(S.binding, S.sessionId)) {
                applyBinding(choice.binding, choice.source);
            }
        }
        function bind(binding) { return applyBinding(binding, 'explicit'); }
        function bindCurrent() {
            const current = S.status && S.status.current_run_id;
            if (current) return applyBinding({kind: 'LIVE', runId: current}, 'current');
        }
        function dismissNewRun() { S.dismissedCurrent = S.status ? S.status.current_run_id : null; }

        // ── Model and derived views ──────────────────────────────────────────
        function model() {
            if (modelCache.version === S.version) return modelCache.model;
            let value = null;
            const scopeFor = () => (S.binding.kind === 'LIVE'
                ? {object_id: objectId(), pair_id: S.pairId, run_id: S.binding.runId} : null);
            if (S.binding && S.binding.kind === 'LIVE' && S.index && Array.isArray(S.index.regions)) {
                value = buildModel({regions: S.index.regions.map(fromIndexRegion), reviews: S.reviews, edits: S.edits,
                    scope: scopeFor()});
            } else if (S.binding && S.binding.kind === 'SNAP' && S.uiData) {
                value = buildModel({regions: S.uiData.regions.map(r => fromUiRegion(r, Core)), reviews: S.reviews,
                    edits: S.edits, scope: null});
            }
            modelCache = {version: S.version, model: value};
            return value;
        }
        function rowChip(row) {
            const m = model();
            if (!m || !row) return null;
            if (chipVersion !== S.version) { chipCache.clear(); chipVersion = S.version; }
            const key = Core.groupKey(row.leftPages, row.rightPages);
            if (!chipCache.has(key)) chipCache.set(key, rowChipOf(m, row));
            return chipCache.get(key);
        }
        function segmentLabel(row) {
            const chip = rowChip(row);
            return chip ? 'Смысловые блоки ◇' + chip.regions : 'Смысловые блоки';
        }
        function summary() {
            const m = model();
            if (!m) return null;
            const c = summaryOf(m);
            const who = S.binding.kind === 'SNAP' ? 'снимок ' + snapShort(S.binding.resultId) : 'прогон ' + short(S.binding.runId);
            return {...c, text: T.SUMMARY(who, c.anchors, c.forbidden, c.attention)};
        }
        function offMapCount(savedLinks) {
            const m = model();
            if (!m) return 0;
            return m.decided.filter(d => Core.placementClass(d.oldPage, d.newPage, savedLinks).kind === 'OFF_MAP').length;
        }
        function untouched(rows) { const m = model(); return m ? Core.untouchedRegions(m.regions, rows) : []; }
        function unpaired(rows) { const m = model(); return m ? Core.unpairedRegions(m.regions, rows) : []; }

        // ── Lens ─────────────────────────────────────────────────────────────
        function lensRowOf(row) {
            return {key: row.key, leftPages: uniqSorted(row.leftPages), rightPages: uniqSorted(row.rightPages),
                explicitLinkIndex: row.explicitLinkIndex === undefined ? null : row.explicitLinkIndex,
                source: row.source || '', confidence: row.confidence || ''};
        }
        function openRow(row) {
            if (!row) return;
            abortPages();
            S.lens = {key: Core.groupKey(row.leftPages, row.rightPages), row: raw(lensRowOf(row)), mode: 'pair', focus: '',
                extra: {OLD: [], NEW: []}};
            S.selection = {OLD: [], NEW: [], link: '', block: null};
            S.rowNotice = null;
            ensureLensPages();
        }
        function enterBlocks(row) {
            S.blocksActive = true;
            const key = row ? Core.groupKey(row.leftPages, row.rightPages) : '';
            if (row && key !== S.lens.key) {
                // A region opened from the map (pinned row, notice) keeps its lens; only the pair context follows.
                if (S.lens.pinned) S.lens = {...S.lens, key, row: raw(lensRowOf(row))};
                else openRow(row);
            }
            if (S.lens.pinned) S.lens = {...S.lens, pinned: false};
            if (S.binding && S.binding.kind === 'SNAP') loadSnapshot();
            ensureLensPages();
        }
        // Rows of the sheet map changed (MASTER §12 step 2): stay on the same composition, else move.
        function syncRows(rows) {
            if (!S.lens.row) return;
            if ((rows || []).some(r => Core.groupKey(r.leftPages, r.rightPages) === S.lens.key)) return;
            const before = S.lens.row;
            const anchorSide = before.leftPages.length ? 'leftPages' : 'rightPages';
            const anchor = before[anchorSide][0];
            const next = (rows || []).find(r => (r[anchorSide] || []).map(Number).includes(anchor));
            const describe = r => 'OLD ' + (joinPages(r.leftPages) || '—') + ' ↔ NEW ' + (joinPages(r.rightPages) || '—');
            if (!next) {
                S.rowNotice = {text: 'Пара листов изменилась', temporary: true};
                return;
            }
            openRow(next);
            S.rowNotice = {text: T.ROW_CHANGED(describe(before), describe(next))};
        }
        function regionById(id) { const m = model(); return m && id ? m.byId.get(id) || null : null; }
        function shelves() {
            const m = model(), row = S.lens.row;
            const out = {both: [], old: [], new: []};
            if (!m || !row) return out;
            for (const region of m.regions) {
                const rel = Core.rel(region, row);
                if (rel === 'NONE') continue;
                const shelf = rel === 'OLD_ONLY' ? 'old' : rel === 'NEW_ONLY' ? 'new' : 'both';
                out[shelf].push(region);
            }
            const attention = r => ((m.info.get(r.id) || {}).attention ? 0 : 1);
            for (const k of Object.keys(out)) out[k].sort((a, b) => attention(a) - attention(b) || a.id.localeCompare(b.id));
            return out;
        }
        function focusId() {
            const m = model();
            if (!m) return '';
            if (S.lens.focus && m.byId.has(S.lens.focus)) return S.lens.focus;
            if (S.lens.mode === 'region' || !S.lens.row) return '';
            const both = shelves().both;
            return both.length ? both[0].id : '';
        }
        function focusRegion(id, mode) {
            S.lens = {...S.lens, focus: id || '', mode: mode || S.lens.mode};
            ensureLensPages();
        }
        // From outside the workspace (pinned row, notice): the next enterBlocks keeps this region lens.
        function openRegion(id, pin = true) {
            S.blocksActive = true;
            S.lens = {...S.lens, pinned: !!pin};
            focusRegion(id, 'region');
        }
        function setLens(mode) {
            if (mode === 'region' && !focusId()) return;
            S.lens = {...S.lens, mode, focus: focusId()};
            ensureLensPages();
        }
        function addExtraPage(side, page) {
            const p = Number(page);
            if (!p || S.lens.extra[side].includes(p)) return;
            S.lens = {...S.lens, extra: {...S.lens.extra, [side]: [...S.lens.extra[side], p]}};
            ensureLensPages();
        }
        function lensPages() {
            const row = S.lens.row;
            const inPair = {OLD: row ? row.leftPages : [], NEW: row ? row.rightPages : []};
            const region = S.lens.mode === 'region' ? regionById(focusId()) : null;
            const pages = {};
            for (const side of SIDES) {
                const list = region ? Core.regionPages(region, side)
                    : [...inPair[side], ...S.lens.extra[side].filter(p => !inPair[side].includes(p))];
                pages[side] = list.map(page => ({page, inPair: inPair[side].includes(page)}));
            }
            return pages;
        }
        function pageLayer() {
            if (S.binding && S.binding.kind === 'SNAP') return 'SNAP';
            if (S.binding && S.binding.kind === 'LIVE' && runUsable(run())) return 'RUN';
            return 'SOURCE';
        }
        function ensureLensPages() {
            if (!S.blocksActive || !S.sessionId || !S.pairId || !S.status) return;
            const layer = pageLayer();
            if (layer === 'SNAP' && !S.uiData) return;
            const pages = lensPages();
            for (const side of SIDES) {
                const target = layer === 'SNAP' ? S.geometry[side] : S.pages[side];
                const missing = pages[side].map(p => p.page).filter(p => !target[p]);
                for (let i = 0; i < missing.length; i += PAGE_CHUNK) requestPages(layer, side, missing.slice(i, i + PAGE_CHUNK));
            }
        }
        async function requestPages(layer, side, pages) {
            if (!pages.length) return;
            const my = bindToken;
            const store = layer === 'SNAP' ? 'geometry' : 'pages';
            const marked = {...S[store][side]};
            for (const p of pages) marked[p] = {status: 'loading'};
            S[store] = {...S[store], [side]: marked};
            const controller = typeof AbortController === 'function' ? new AbortController() : null;
            if (controller) controllers.push(controller);
            try {
                const body = layer === 'RUN'
                    ? await client.runPageBlocks(S.sessionId, S.pairId, S.binding.runId, side, pages, controller && controller.signal)
                    : await client.sourcePageBlocks(S.sessionId, S.pairId, side, pages, controller && controller.signal);
                if (my !== bindToken) return;
                const next = {...S[store][side]};
                for (const row of (body && body.pages) || []) next[row.physical_page] = {status: 'ok', row: raw(row)};
                for (const p of pages) if (!next[p] || next[p].status === 'loading') next[p] = {status: 'error', reason: 'PAGE_MISSING'};
                S[store] = {...S[store], [side]: next};
            } catch (error) {
                if (my !== bindToken) return;
                const next = {...S[store][side]};
                if (error && error.name === 'AbortError') {
                    for (const p of pages) if (next[p] && next[p].status === 'loading') delete next[p];
                } else {
                    const reason = error && error.code === 'SOURCE_BLOCKS_UNAVAILABLE' ? 'SOURCE_BLOCKS_UNAVAILABLE' : 'READ_FAILED';
                    for (const p of pages) next[p] = {status: 'error', reason};
                    if (reason === 'READ_FAILED' && layer !== 'SNAP') S.pageError = {kind: 'FAIL14'};
                }
                S[store] = {...S[store], [side]: next};
            } finally {
                controllers = controllers.filter(c => c !== controller);
            }
        }
        function snapBlocks(side, page) {
            const seen = new Map();
            for (const region of (S.uiData && S.uiData.regions) || []) {
                for (const p of ((region.pages || {})[side] || [])) {
                    if (Number(p.page) !== Number(page)) continue;
                    for (const b of p.blocks || []) {
                        if (!seen.has(b.id)) seen.set(b.id, {block_id: b.id, modality: b.type, source_block_type: null, bbox: b.bbox});
                    }
                }
            }
            return [...seen.values()];
        }
        function pageView(side, page) {
            const layer = pageLayer();
            if (layer === 'SNAP') {
                const g = S.geometry[side][page];
                return {status: 'ok', blocks: snapBlocks(side, page), geometry: g && g.status === 'ok' ? g.row.geometry : null,
                    reason: null};
            }
            const entry = S.pages[side][page];
            if (!entry || entry.status === 'loading') return {status: 'loading', blocks: [], geometry: null, reason: null};
            if (entry.status === 'error') return {status: 'error', blocks: [], geometry: null, reason: entry.reason};
            const row = entry.row;
            if (!row.available) return {status: 'error', blocks: [], geometry: row.geometry, reason: row.reason};
            return {status: 'ok', blocks: row.blocks || [], geometry: row.geometry, reason: null};
        }
        function pageText(side, page, view) {
            if (!view || view.status !== 'error') return '';
            if (view.reason === 'PAGE_OUT_OF_RANGE') return T.FAIL3(page);
            if (view.reason === 'SOURCE_BLOCKS_UNAVAILABLE') return T.FAIL2(page);
            if (view.reason === 'PAGE_JSON_MISSING' || view.reason === 'PAGE_JSON_UNREADABLE') return T.FAIL1(bindingLabel());
            return T.FAIL14;
        }
        function blocksFor(side, page) {
            const m = model();
            return visibleBlocks(side, pageView(side, page).blocks, {showStamps: S.showStamps,
                linkEnds: m ? m.linkEnds : null, reviewSel: m ? m.reviewSel : null});
        }
        function rasterUrl(side, page) {
            const r = run();
            if (S.binding && S.binding.kind === 'LIVE' && r && r.pdf_match !== true && objectId()) {
                // K-PDF only: the live PDF is not the run's source, so show the run's own raster (C9).
                return hmAssetUrl(objectId(), S.pairId, S.sessionId, S.binding.runId, side, page);
            }
            const width = lensPages()[side].length > 1 ? STACK_PREVIEW_WIDTH : PAGE_PREVIEW_WIDTH;
            // The viewer's own URL builder (scPagePreviewUrl + scPageSignatures) shares the browser cache.
            // Its signature is one per PDF side; a URL once built is kept while the viewer reloads it.
            const key = `${side}|${page}|${width}`;
            let built = '';
            try { built = previewBuilder ? previewBuilder(VIEWER_SIDE[side], Number(page), width) : ''; } catch (_) { built = ''; }
            // null: the viewer is still reading this PDF's signature — wait rather than open a second cache key.
            if (built === null) return previewCache.get(key) || '';
            built = String(built || '');
            if (built) previewCache.set(key, built);
            else built = previewCache.get(key) || '';
            if (built) return built;
            const src = S.status && S.status.source_blocks && S.status.source_blocks[side];
            return previewUrl(S.sessionId, S.pairId, side, page, src && src.pdf_sha256, width);
        }
        function setPreviewUrlBuilder(fn) {
            previewBuilder = typeof fn === 'function' ? fn : null;
            previewCache = new Map();
        }
        function sourceSummary() {
            let hidden = 0;
            const counts = {OLD: 0, NEW: 0};
            const pages = lensPages();
            for (const side of SIDES) {
                for (const {page} of pages[side]) {
                    const all = pageView(side, page).blocks;
                    const shown = blocksFor(side, page);
                    counts[side] += shown.length;
                    hidden += all.length - shown.length;
                }
            }
            return T.F_SUMMARY(counts.OLD, counts.NEW, !S.showStamps && hidden > 0);
        }

        // Failure 11: snapshot members absent from this version's recognition (display only).
        function snapshotMismatch() {
            if (pageLayer() !== 'SNAP' || !S.uiData) return 0;
            const pages = lensPages();
            let missing = 0;
            for (const side of SIDES) {
                for (const {page} of pages[side]) {
                    const g = S.geometry[side][page];
                    if (!g || g.status !== 'ok' || !g.row.available) continue;
                    const known = new Set((g.row.blocks || []).map(b => b.block_id));
                    missing += snapBlocks(side, page).filter(b => !known.has(b.block_id)).length;
                }
            }
            return missing;
        }

        // ── Edges in the lens ────────────────────────────────────────────────
        function lensEdges() {
            const m = model();
            if (!m) return {edges: [], total: 0, truncated: false, offLens: new Map()};
            const pages = lensPages();
            const stack = {OLD: pages.OLD.map(p => p.page), NEW: pages.NEW.map(p => p.page)};
            const virtual = {leftPages: stack.OLD, rightPages: stack.NEW};
            const focus = focusId();
            let regionIds;
            if (S.lens.mode === 'region') regionIds = focus ? [focus] : [];
            else if (S.focusOnly) regionIds = focus ? [focus] : [];
            else regionIds = m.regions.filter(r => Core.rel(r, virtual) !== 'NONE').map(r => r.id);
            const wanted = new Set(regionIds);
            const offLens = new Map();
            let edges = [];
            for (const edge of m.edges.values()) {
                const ids = edge.regionIds.filter(id => wanted.has(id));
                if (!ids.length) continue;
                const onStack = stack.OLD.includes(edge.oldPage) || stack.NEW.includes(edge.newPage);
                if (!onStack) {
                    for (const id of ids) offLens.set(id, (offLens.get(id) || 0) + 1);
                    continue;
                }
                edges.push({...edge, focused: !focus || edge.regionIds.includes(focus)});
            }
            const total = edges.length;
            let truncated = false;
            if (total > EDGE_LIMIT && !S.showAllEdges) {
                const sel = S.selection;
                edges = edges.filter(e => sel.OLD.includes(e.old_block_id) || sel.NEW.includes(e.new_block_id) || e.key === sel.link);
                truncated = true;
            }
            return {edges, total, truncated, offLens};
        }
        function edgeView(edge) {
            const focus = focusId();
            const own = edge.links.find(l => l.region_id === focus);
            const state = edge.conflict ? 'CROSS_REGION_CONFLICT' : (own ? own.state : edge.state);
            const origin = edge.links[0] || {};
            const kind = origin.source === 'HUMAN_MANUAL' ? 'human'
                : String(origin.membership_kind || '').startsWith('INHERITED_GROUP') ? 'inherited' : 'ai';
            const m = model();
            const typeOf = (side, id) => ((m && m.blockType.get(side + '|' + id)) || {}).type;
            const title = `OLD ${modality(typeOf('OLD', edge.old_block_id))} стр. ${edge.oldPage ?? '—'} → NEW `
                + `${modality(typeOf('NEW', edge.new_block_id))} стр. ${edge.newPage ?? '—'} · ${STATE_LABEL[state] || state}`
                + ` · ${edge.regionIds.join(', ')}`;
            return {state, kind, glyph: GLYPH[state] || '', title, label: STATE_LABEL[state] || state,
                origin: origin.source === 'HUMAN_MANUAL' ? 'вручную' : 'ИИ'};
        }

        // ── Selection and block cards ────────────────────────────────────────
        function selectBlock(side, blockId, page) {
            const list = S.selection[side];
            const next = list.includes(blockId) ? list.filter(id => id !== blockId) : [...list, blockId];
            S.selection = {...S.selection, [side]: next, block: {side, id: blockId, page: Number(page) || null}, link: ''};
            loadDetail(side, blockId, page);
        }
        function selectLink(key) {
            S.selection = {...S.selection, link: S.selection.link === key ? '' : key, block: null};
            const m = model();
            const edge = m && m.edges.get(key);
            if (edge && S.selection.link) {
                const marks = {...S.ctxMarks};
                for (const l of edge.links) {
                    const k = ctxKey(l.link_id);
                    if (marks[k]) { delete marks[k]; try { if (storage) storage.removeItem(k); } catch (_) { /* optional */ } }
                }
                S.ctxMarks = marks;
            }
        }
        function clearSelection() { S.selection = {OLD: [], NEW: [], link: '', block: null}; }
        const detailKey = (side, id) => `${bindingKey(S.binding, S.sessionId) || 'SRC'}|${side}|${id}`;
        async function loadDetail(side, blockId, page) {
            const key = detailKey(side, blockId);
            if (S.details[key]) return;
            const layer = pageLayer();
            if (layer === 'SNAP') {
                let found = null;
                for (const region of (S.uiData && S.uiData.regions) || []) {
                    const members = side === 'OLD' ? region.old_blocks : region.new_blocks;
                    found = (members || []).find(b => b.id === blockId)
                        || ((region.pages || {})[side] || []).flatMap(p => p.blocks || []).find(b => b.id === blockId) || null;
                    if (found) break;
                }
                S.details = {...S.details, [key]: found ? {status: 'ok', data: raw({modality: found.type,
                    physical_page: found.page ?? page, structured_md: found.structured_md || '', tables: found.tables || []})}
                    : {status: 'error'}};
                return;
            }
            const my = bindToken;
            S.details = {...S.details, [key]: {status: 'loading'}};
            try {
                const data = layer === 'RUN'
                    ? await client.runBlock(S.sessionId, S.pairId, S.binding.runId, side, blockId, page)
                    : await client.sourceBlock(S.sessionId, S.pairId, side, blockId);
                if (my !== bindToken) return;
                S.details = {...S.details, [key]: {status: 'ok', data: raw(data)}};
            } catch (_) {
                if (my !== bindToken) return;
                S.details = {...S.details, [key]: {status: 'error'}};
            }
        }
        function detail(side, blockId) { return S.details[detailKey(side, blockId)] || null; }

        // ── Journal (file order; superseded reviews grey) ────────────────────
        function journal(regionId) {
            const m = model();
            const reviews = (S.reviews || []).filter(r => !regionId || r.region_id === regionId);
            const edits = (S.edits || []).filter(e => !regionId || e.region_id === regionId);
            const foreign = row => (S.binding && S.binding.kind === 'SNAP' ? row.result_id !== S.binding.resultId
                : S.binding ? row.run_id !== S.binding.runId : false);
            const foreignText = S.binding && S.binding.kind === 'SNAP' ? T.OUTSIDE_SNAPSHOT : T.OUTSIDE_RESULT;
            const mark = row => {
                const code = m && m.issueById.get(row.review_id || row.event_id);
                return code && T.MARK[code] ? T.MARK[code] : '';
            };
            const byRegion = new Map();
            for (const r of reviews) {
                if (!byRegion.has(r.region_id)) byRegion.set(r.region_id, []);
                byRegion.get(r.region_id).push(r);
            }
            const entries = [];
            reviews.forEach((r, order) => {
                const list = byRegion.get(r.region_id);
                const next = list[list.indexOf(r) + 1];
                entries.push({key: 'r' + order, order, file: 0, time: r.timestamp, when: dayMonthTime(r.timestamp),
                    region: r.region_id, what: REVIEW_LABEL[r.status] || r.status,
                    size: `выборка OLD ${(r.old_block_ids || []).length} · NEW ${(r.new_block_ids || []).length}`,
                    comment: r.comment || '', superseded: next ? 'заменено ' + dayMonthTime(next.timestamp) : '',
                    foreign: foreign(r) ? foreignText : '', mark: mark(r), ids: `${r.run_id || r.result_id || '—'}`});
            });
            edits.forEach((e, order) => {
                entries.push({key: 'e' + order, order, file: 1, time: e.timestamp, when: dayMonthTime(e.timestamp),
                    region: e.region_id, what: EVENT_LABEL[e.event_type] || e.event_type,
                    size: `OLD ${e.old_block_id} → NEW ${e.new_block_id}`, comment: e.comment || '', superseded: '',
                    foreign: foreign(e) ? foreignText : '', mark: mark(e), ids: `${e.run_id || e.result_id || '—'}`});
            });
            const t = entry => { const x = Core.eventTime({timestamp: entry.time}); return x ? x.s * 1e6 + x.us : Infinity; };
            return entries.sort((a, b) => t(a) - t(b) || a.file - b.file || a.order - b.order);
        }
        function currentDecision(regionId) {
            const m = model();
            const i = m && m.info.get(regionId);
            if (!i || !i.review) return 'Текущее решение: без решения.';
            const tail = i.status === 'HUMAN_CONFIRMED' ? ` · закреплено ${i.anchors}`
                : i.status === 'HUMAN_REJECTED' ? ` · запрещено ${i.forbidden}` : '';
            return T.B_CURRENT(`${REVIEW_LABEL[i.status] || i.status} ${dayMonthTime(i.review.timestamp)}`, tail);
        }
        function regionStatusLabel(regionId) {
            const m = model();
            const i = m && m.info.get(regionId);
            if (!i || i.status === 'UNREVIEWED') return 'без решения';
            if (i.status === 'HUMAN_UNCERTAIN') return 'не уверен';
            if (i.status === 'HUMAN_REJECTED') {
                return i.forbidden ? `${plural(i.forbidden, 'Запрещена', 'Запрещены', 'Запрещены')} ${i.forbidden} `
                    + plural(i.forbidden, 'точная связь', 'точные связи', 'точных связей')
                    : 'Отклонение ничего не запретило: в выборке нет связей';
            }
            return i.anchors ? `✓ закреплено ${i.anchors}` : 'решение без связей: 0 ограничений';
        }

        // ── Placement notice and session marks (MASTER §12) ──────────────────
        const ctxKey = linkId => `sbm:ctx:${S.binding && S.binding.kind === 'SNAP' ? S.binding.resultId : S.binding ? S.binding.runId : ''}:${linkId}`;
        function noteSheetLinks(pairId, links) {
            const copy = (links || []).map(l => ({left_pages: uniqSorted(l.left_pages), right_pages: uniqSorted(l.right_pages)}));
            if (!pairId || pairId !== S.pairId || baselinePair !== pairId || linksBaseline === null) {
                linksBaseline = copy;
                baselinePair = pairId || '';
                return;
            }
            const before = linksBaseline;
            linksBaseline = copy;
            if (JSON.stringify(before) === JSON.stringify(copy)) return;
            const m = model();
            if (!m) return;
            const notice = placementNotice(m.decided, before, copy, bindingLabel());
            if (!notice) return;
            const marks = {...S.ctxMarks};
            for (const d of notice.changed) {
                const key = ctxKey(d.link_id);
                marks[key] = true;
                try { if (storage) storage.setItem(key, '1'); } catch (_) { /* storage is optional */ }
            }
            S.ctxMarks = marks;
            S.notice = {text: notice.text, target: notice.changed[0]};
        }
        function contextMarked(linkId) {
            const key = ctxKey(linkId);
            if (S.ctxMarks[key]) return true;
            try { return !!(storage && storage.getItem(key)); } catch (_) { return false; }
        }
        function showNoticeTarget(pin = true) {
            const target = S.notice && S.notice.target;
            S.notice = null;
            if (!target) return;
            openRegion(target.region_id, pin);
            selectLink(target.key);
        }
        function dismissNotice() { S.notice = null; }

        // ── Bridge check (GET, only by the button) ───────────────────────────
        async function bridgeCheck() {
            const r = run();
            if (!r || S.bridge.status === 'loading') return;
            const my = bindToken;
            S.bridge = {...S.bridge, status: 'loading'};
            try {
                const body = await client.bridgeCheck(S.sessionId, S.pairId, S.binding.runId);
                if (my !== bindToken) return;
                let text, conflict = false;
                if (body.ok && body.result) {
                    const a = body.result.confirmed_anchor_count, f = body.result.rejected_link_count;
                    text = a + f ? T.BRIDGE_OK(a, f) : T.BRIDGE_EMPTY;
                } else {
                    // The bridge puts the specific cause in error.reason; error.code is its generic class
                    // (BRIDGE_CONFLICT_REVIEW_REQUIRED / BRIDGE_MAPPING_REJECTED).
                    const err = body.error || {};
                    const code = err.reason || err.code || 'INVALID_SOURCE_OR_HISTORY';
                    text = T.BRIDGE[code] || T.BRIDGE_OTHER(code);
                    conflict = code === 'CONFIRMED_AND_REJECTED_EXACT_EDGE';
                }
                S.bridge = {status: 'done', text, conflict, cached: !!body.cached, checkedAt: clock(body.checked_at),
                    details: body.result ? body.result.review_snapshot_sha256 : ''};
            } catch (_) {
                if (my !== bindToken) return;
                S.bridge = {status: 'error', text: T.FAIL14, details: '', checkedAt: '', cached: false, conflict: false};
            }
        }
        function showConflict() {
            const m = model();
            const edge = m && m.conflicts[0];
            if (!edge) return;
            const anchor = edge.links.find(l => l.state === 'ANCHOR');
            openRegion(anchor ? anchor.region_id : edge.regionIds[0], false);
            selectLink(edge.key);
        }

        // ── Messages by state (UX_STATE_MATRIX §0 order, §2 grouping) ────────
        function writeBlockText(r) {
            const label = short(r.run_id);
            switch (r.write_block_reason) {
                case 'RUN_INVALID': return T.FAIL4(label);
                case 'HM_UNAVAILABLE': return hmFailureKind(r.hm_reason) === 'FAIL5' ? T.FAIL5(label) : T.FAIL1(label);
                case 'SOURCE_PDF_CHANGED': return T.K_PDF(label);
                case 'SOURCE_BLOCKS_CHANGED': return T.K_BLOCKS(label);
                case 'HISTORY_POISONED': {
                    const m = model();
                    const n = m && m.eventIssues ? m.eventIssues : Math.max(1, (r.history && r.history.poison_codes || []).length);
                    return T.M1(n);
                }
                case 'WRITES_DISABLED': return T.L;
                default: return '';
            }
        }
        // M1: events the bridge would refuse, shown (never hidden), with the client's display marks.
        function poisonDetails() {
            const m = model();
            if (!m) return [];
            return [...(S.reviews || []), ...(S.edits || [])].filter(e => m.issueById.has(e.review_id || e.event_id)).map(e => {
                const code = m.issueById.get(e.review_id || e.event_id);
                const mark = T.MARK[code] || (code === 'HUMAN_EVENT_SCOPE_MISMATCH' ? T.OUTSIDE_RESULT : code);
                return `${dayMonthTime(e.timestamp)} · ${e.region_id || '—'} · ${REVIEW_LABEL[e.status] || EVENT_LABEL[e.event_type] || '—'} · ${mark}`;
            });
        }
        function failureText(kind) {
            const label = bindingLabel();
            return {FAIL1: T.FAIL1(label), FAIL4: T.FAIL4(label), FAIL5: T.FAIL5(label), FAIL14: T.FAIL14}[kind] || T.FAIL14;
        }
        function hmHref() {
            if (S.binding && S.binding.kind === 'SNAP') { const s = snapshot(); return (s && s.hm_url) || ''; }
            if (S.binding && S.binding.kind === 'LIVE' && objectId()) return humanMappingHref(objectId(), S.pairId, S.sessionId, S.binding.runId);
            return '';
        }
        function sideLabel(side, pages, sheetOf) {
            const list = uniqSorted(pages);
            const sheets = list.map(p => sheetOf(side, p));
            if (list.length === 1) return sheets[0] ? `лист ${sheets[0]} (стр. ${list[0]})` : `стр. ${list[0]}`;
            return sheets.every(Boolean) ? `листы ${sheets.join(', ')} (стр. ${list.join(', ')})` : `стр. ${list.join(', ')}`;
        }
        function messages({rows = [], mapBuilt = true, sheetOf = () => ''} = {}) {
            const out = {banners: [], conflict: null, context: [], hints: [], footer: [], header: ''};
            const st = S.status;
            if (S.statusError) {
                out.banners.push({key: 'fail14', tone: 'error', text: T.FAIL14, actions: [{id: 'retry', label: 'Повторить'}]});
                return out;
            }
            if (!st) return out;
            const r = run(), m = model(), latest = st.latest_attempt;
            const href = hmHref();
            // 1 · write block (first reason in the canonical order) or the snapshot constant.
            if (S.binding && S.binding.kind === 'SNAP') {
                out.banners.push({key: 'gsnap', tone: 'info', text: T.G_SNAP,
                    actions: href ? [{id: 'href', label: 'Открыть в Human Mapping ↗', href}] : []});
            } else if (S.binding && !r) {
                out.banners.push({key: 'fail4', tone: 'error', text: T.FAIL4(short(S.binding.runId)), actions: [{id: 'choose', label: 'Выбрать результат'}]});
            } else if (r && r.write_block_reason) {
                const text = writeBlockText(r);
                const more = (r.write_block_reasons || []).slice(1).map(code => writeBlockText({...r, write_block_reason: code}));
                if (r.write_block_reason === 'HISTORY_POISONED') more.unshift(...poisonDetails());
                out.banners.push({key: 'write', tone: r.write_block_reason === 'WRITES_DISABLED' ? 'info' : 'error', text,
                    details: more.filter(Boolean),
                    link: r.write_block_reason === 'WRITES_DISABLED' && href ? {label: 'Human Mapping ↗', href} : null});
            }
            for (const kind of [S.dataError && S.dataError.kind, S.historyError && S.historyError.kind, S.pageError && S.pageError.kind]) {
                if (!kind) continue;
                const text = failureText(kind);
                if (out.banners.some(b => b.text === text)) continue;
                out.banners.push({key: 'data-' + kind, tone: 'error', text,
                    actions: kind === 'FAIL14' ? [{id: 'retry', label: 'Повторить'}] : []});
            }
            // 2 · result changes: J, N, F2.
            if (latest && latest.state === 'RUNNING') {
                out.banners.push({key: 'j', tone: 'info', group: 2,
                    text: S.binding && S.binding.kind === 'LIVE' ? T.J_RUN(short(S.binding.runId)) : T.J_NORUN});
            }
            if (S.bindingSource === 'current' && S.binding && st.current_run_id && st.current_run_id !== S.binding.runId
                    && st.current_run_id !== S.dismissedCurrent) {
                out.banners.push({key: 'n', tone: 'warn', group: 2, text: T.N(short(st.current_run_id), short(S.binding.runId)),
                    actions: [{id: 'go-new', label: 'Перейти к новому'}, {id: 'stay', label: 'Остаться'}]});
            }
            const currentRun = statusRuns().find(x => x.run_id === st.current_run_id);
            if (latest && latest.state === 'FAILED' && st.current_run_id && latest.run_id !== st.current_run_id && currentRun) {
                out.banners.push({key: 'f2', tone: 'warn', group: 2, text: T.F2(dayMonthTime(latest.completed_at || latest.created_at),
                    short(currentRun.run_id), dayMonth(currentRun.completed_at || currentRun.created_at))});
            }
            // 3 · cross-region conflict (M2).
            if (m && m.conflicts.length) {
                const edge = m.conflicts[0];
                const a = edge.links.find(l => l.state === 'ANCHOR'), f = edge.links.find(l => l.state === 'FORBIDDEN');
                out.conflict = {key: 'm2', tone: 'error', text: T.M2(a.region_id, f.region_id),
                    actions: [{id: 'show-conflict', label: 'Показать конфликт'}]};
            }
            // 4 · row context: I, O, P, C/D/E.
            const row = S.lens.row;
            if (!mapBuilt) out.context.push({key: 'i', text: S.binding ? T.I_RUN : T.I_NORUN});
            if (row && S.lens.mode === 'pair') {
                const two = Core.isTwoSided(row);
                if (!two) {
                    const side = row.leftPages.length ? 'OLD' : 'NEW';
                    out.context.push({key: 'p', text: T.P(side + ' ' + sideLabel(side, side === 'OLD' ? row.leftPages : row.rightPages, sheetOf))});
                } else {
                    if (row.explicitLinkIndex === null) out.context.push({key: 'o', text: T.O});
                    const n = m ? m.regions.filter(x => Core.rel(x, row) !== 'NONE').length : null;
                    const card = row.leftPages.length === 1 && row.rightPages.length === 1 ? ''
                        : ` · ${row.leftPages.length}→${row.rightPages.length}`;
                    out.header = `Пара листов: OLD ${sideLabel('OLD', row.leftPages, sheetOf)} ↔ NEW `
                        + `${sideLabel('NEW', row.rightPages, sheetOf)}${card}` + (n === null ? '' : ` · регионов: ${n}`);
                }
                if (row.leftPages.length > MANY_PAGES || row.rightPages.length > MANY_PAGES) out.context.push({key: 'e', text: T.E_MANY});
            }
            if (S.rowNotice) out.context.push({key: 'row-changed', text: S.rowNotice.text});
            // 5 · hints: A / N↔N, B (◌), K0, F, F1, failure 11.
            const noRuns = !statusRuns().some(x => TERMINAL.includes(x.state) && x.hm_available)
                && !(st.snapshots || []).some(s => s.hm_available);
            if (!S.binding) {
                if (latest && latest.state === 'FAILED') {
                    const reason = T.F1_REASON[latest.reason_code] || T.F1_OTHER;
                    out.hints.push({key: 'f1', text: T.F1(dayMonthTime(latest.completed_at || latest.created_at), reason),
                        details: T.F1_REASON[latest.reason_code] ? [] : [String(latest.reason_code || '—')]});
                }
                if (noRuns && !(latest && ['FAILED', 'RUNNING'].includes(latest.state))) {
                    out.hints.push({key: 'f', text: T.F, actions: [{id: 'launch', label: 'К запуску анализа'}]});
                } else if (latest && latest.state === 'FAILED') {
                    out.hints.push({key: 'f', text: T.F, actions: [{id: 'launch', label: 'К запуску анализа'}]});
                }
            }
            if (r && r.source_stale && r.blocks_content_match === true && r.pdf_match === true) out.hints.push({key: 'k0', text: T.K0});
            const focus = focusId();
            if (m && row && S.lens.mode === 'pair' && Core.isTwoSided(row)) {
                const touching = m.regions.filter(x => Core.rel(x, row) !== 'NONE');
                const quiet = touching.every(x => !m.latest.get(x.id)
                    && !(m.edits || []).some(e => e.region_id === x.id));
                const fr = focus && m.byId.get(focus);
                const frInfo = fr && m.info.get(fr.id);
                if (quiet && touching.length && row.explicitLinkIndex !== null) {
                    out.hints.push({key: 'a', text: fr && fr.member_cardinality === 'N:N' && frInfo && !frInfo.links
                        ? T.A_NN(fr.id) : T.A(touching.length)});
                }
            }
            if (m && focus && (m.info.get(focus) || {}).newer) out.hints.push({key: 'b', text: T.B_NEWER(m.info.get(focus).newer)});
            const mismatch = snapshotMismatch();
            if (mismatch) out.hints.push({key: 'fail11', text: T.FAIL11(mismatch)});
            // footer: G-LIVE / H / persistent line, or the snapshot footer.
            if (S.binding && S.binding.kind === 'LIVE' && r) {
                const reviews = (S.reviews || []).length, events = (S.edits || []).length;
                if (reviews + events) out.footer.push({key: 'glive', text: T.G_LIVE(short(r.run_id), reviews + events, reviews, events)});
                if (r.projectchange_count != null) out.footer.push({key: 'h', text: T.H(r.projectchange_count)});
                out.footer.push({key: 'footer', text: T.FOOTER(short(r.run_id))});
            } else if (S.binding && S.binding.kind === 'SNAP') {
                out.footer.push({key: 'gsnap-footer', text: T.G_SNAP_FOOTER(snapShort(S.binding.resultId))});
            }
            return out;
        }
        function bindingChip() {
            if (!S.binding) {
                return bindingOptions().length ? 'Результат анализа не выбран · блоки распознавания ▾'
                    : 'Анализа этой пары ещё не было · блоки распознавания · только просмотр';
            }
            if (S.binding.kind === 'SNAP') return `Снимок ${snapShort(S.binding.resultId)} · только просмотр ▾`;
            const r = run();
            if (!r) return `Прогон ${short(S.binding.runId)} ▾`;
            return runLabel(r) + ' ▾';
        }
        function runLabel(r) {
            return [`Прогон ${short(r.run_id)}`, dayMonth(r.completed_at || r.created_at), r.model_display || r.model,
                r.engine_version ? 'V3 ' + r.engine_version : '', r.is_current ? 'текущий' : ''].filter(Boolean).join(' · ');
        }
        function bindingOptions() {
            const runs = statusRuns().map(r => ({kind: 'LIVE', id: r.run_id, label: runLabel(r),
                active: !!S.binding && S.binding.kind === 'LIVE' && S.binding.runId === r.run_id}));
            const snaps = ((S.status && S.status.snapshots) || []).map(s => ({kind: 'SNAP', id: s.result_id,
                label: `Снимок ${snapShort(s.result_id)} · только просмотр`,
                active: !!S.binding && S.binding.kind === 'SNAP' && S.binding.resultId === s.result_id}));
            return [...runs, ...snaps];
        }

        const store = {
            state: S, T, load, retry, refreshStatus, setCatalogFocus, bind, bindCurrent, dismissNewRun,
            model, rowChip, segmentLabel, summary, offMapCount, untouched, unpaired,
            openRow, enterBlocks, syncRows, shelves, focusId, focusRegion, openRegion, setLens, addExtraPage,
            lensPages, pageLayer, pageView, pageText, blocksFor, rasterUrl, setPreviewUrlBuilder, sourceSummary, snapshotMismatch,
            lensEdges, edgeView, selectBlock, selectLink, clearSelection, detail, journal, currentDecision,
            regionStatusLabel, noteSheetLinks, contextMarked, showNoticeTarget, dismissNotice, bridgeCheck, showConflict,
            messages, bindingChip, bindingOptions, hmHref, run, bindingLabel,
            regionById, stateLabel: s => STATE_LABEL[s] || s,
        };
        return markRaw(store);
    }

    // ── Components ────────────────────────────────────────────────────────────
    function layoutFor(width) {
        if (width >= 1280) return 'full';
        if (width >= 900) return 'compact';
        if (width >= 600) return 'single';
        return 'list';
    }

    const WORKSPACE_TEMPLATE = `
<section class="sbm-workspace" :class="'sbm-layout--' + layout" aria-label="Смысловые блоки" @keydown.esc="store.clearSelection()">
    <header class="sbm-bar">
        <details class="sbm-binding" :class="{'is-empty': !S.binding}">
            <summary :title="S.binding ? 'Выбрать результат анализа' : ''">{{ store.bindingChip() }}</summary>
            <div v-if="bindingOptions.length" class="sbm-menu" role="menu">
                <button v-for="opt in bindingOptions" :key="opt.kind + opt.id" type="button" role="menuitem"
                        :class="{'is-active': opt.active}" @click="choose(opt, $event)">{{ opt.label }}</button>
            </div>
            <p v-else class="sbm-muted">Результатов анализа этой пары нет.</p>
        </details>
        <div v-if="model" class="sbm-lens" role="group" aria-label="Линза">
            <button type="button" :class="{'is-active': S.lens.mode === 'pair'}" :disabled="!S.lens.row"
                    @click="store.setLens('pair')">Эта пара</button>
            <button type="button" :class="{'is-active': S.lens.mode === 'region'}" :disabled="!focus"
                    :title="focus ? '' : 'Выберите регион на полке'" @click="store.setLens('region')">Весь регион</button>
        </div>
        <span v-if="twoSided.length && S.lens.row" class="sbm-pairnav">
            <button type="button" class="sbm-link" :disabled="pairIndex <= 0" aria-label="Предыдущая пара листов" @click="stepPair(-1)">‹</button>
            пара {{ pairIndex + 1 || '—' }}/{{ twoSided.length }}
            <button type="button" class="sbm-link" :disabled="pairIndex < 0 || pairIndex >= twoSided.length - 1" aria-label="Следующая пара листов" @click="stepPair(1)">›</button>
        </span>
        <details v-if="model" class="sbm-nav" :open="navOpen" @toggle="navOpen = $event.target.open">
            <summary>Все регионы ({{ model.regions.length }}) ▾</summary>
            <div class="sbm-menu sbm-nav__menu">
                <div class="sbm-nav__filters" role="group" aria-label="Фильтр регионов">
                    <button v-for="f in navFilters" :key="f.id" type="button" :class="{'is-active': navFilter === f.id}"
                            :title="f.hint || ''" @click="navFilter = f.id">{{ f.label }}</button>
                </div>
                <button v-for="r in navList" :key="r.id" type="button" class="sbm-nav__item" :title="r.domain"
                        @click="openRegion(r.id)">{{ navLabel(r) }}</button>
                <p v-if="!navList.length" class="sbm-muted">Регионов нет.</p>
            </div>
        </details>
        <label class="sbm-toggle"><input type="checkbox" v-model="S.showStamps"> Показать штампы</label>
        <label v-if="model && S.lens.mode === 'pair'" class="sbm-toggle"><input type="checkbox" v-model="S.focusOnly"> Только фокусный регион</label>
        <button v-if="store.run() && S.binding.kind === 'LIVE'" type="button" class="btn btn-sm btn-secondary"
                :disabled="S.bridge.status === 'loading'" @click="store.bridgeCheck()">Проверка для нового анализа</button>
    </header>
    <div v-if="S.bridge.status === 'done' || S.bridge.status === 'error'" class="sbm-note" role="status">
        {{ S.bridge.text }}<template v-if="S.bridge.cached"> · проверено в {{ S.bridge.checkedAt }}</template>
        <button v-if="S.bridge.conflict" type="button" class="sbm-link" @click="store.showConflict()">Показать конфликт</button>
        <details v-if="S.bridge.details" class="sbm-more"><summary>Подробнее</summary><code>{{ S.bridge.details }}</code></details>
    </div>
    <div v-for="m in msgs.banners" :key="m.key" class="sbm-banner" :class="'sbm-banner--' + m.tone" role="status">
        <span v-if="m.link">{{ m.text.slice(0, m.text.length - m.link.label.length) }}<a class="sbm-link" :href="m.link.href" target="_blank" rel="noopener">{{ m.link.label }}</a></span>
        <span v-else>{{ m.text }}</span>
        <template v-for="a in m.actions || []" :key="a.id">
            <a v-if="a.href" class="sbm-link" :href="a.href" target="_blank" rel="noopener">{{ a.label }}</a>
            <button v-else type="button" class="sbm-link" @click="act(a.id)">{{ a.label }}</button>
        </template>
        <details v-if="m.details && m.details.length" class="sbm-more"><summary>Подробнее</summary>
            <p v-for="(d, i) in m.details" :key="i">{{ d }}</p></details>
    </div>
    <div v-if="S.notice" class="sbm-banner sbm-banner--info" role="status"><span>{{ S.notice.text }}</span>
        <button type="button" class="sbm-link" @click="store.showNoticeTarget(false)">Показать</button></div>
    <div v-if="msgs.conflict" class="sbm-banner sbm-banner--error" role="alert"><span>{{ msgs.conflict.text }}</span>
        <button type="button" class="sbm-link" @click="store.showConflict()">Показать конфликт</button></div>
    <div v-if="S.loading && !S.status" class="sbm-muted sbm-pad">Загрузка смысловых блоков…</div>
    <div v-if="msgs.header" class="sbm-rowline"><strong>{{ msgs.header }}</strong></div>
    <p v-for="c in msgs.context" :key="c.key" class="sbm-context">{{ c.text }}</p>
    <div v-if="!S.binding && S.blocksActive && S.status" class="sbm-rowline sbm-muted">{{ store.sourceSummary() }}</div>
    <div v-if="model && S.lens.mode === 'pair' && S.lens.row && layout === 'full'" class="sbm-shelves">
        <div v-for="shelf in shelfList" :key="shelf.id" class="sbm-shelf">
            <span class="sbm-shelf__name">{{ shelf.label }}:</span>
            <span v-if="!shelf.items.length" class="sbm-muted">—</span>
            <button v-for="r in shelf.items" :key="r.id" type="button" class="sbm-region" :class="{'is-focus': r.id === focus, 'is-attention': attention(r.id)}"
                    :title="regionTitle(r, shelf.id)" @click="store.focusRegion(r.id)" @dblclick="openRegion(r.id)">
                {{ regionChip(r) }}</button>
        </div>
    </div>
    <label v-else-if="model && S.lens.mode === 'pair' && S.lens.row && layout !== 'list'" class="sbm-regionpick">Регион:
        <select :value="focus" @change="store.focusRegion($event.target.value)">
            <option v-for="r in shelfRegions" :key="r.id" :value="r.id">{{ r.id }} · {{ clip(r.domain, 60) }}</option>
        </select></label>
    <div v-if="model && S.lens.mode === 'region' && focusRegion" class="sbm-rowline">
        <strong>◆ {{ focusRegion.id }} «{{ clip(focusRegion.domain, 80) }}» · {{ cardinality(focusRegion) }} · членов OLD {{ focusRegion.old_blocks.length }} / NEW {{ focusRegion.new_blocks.length }}</strong>
        <span class="sbm-muted"> · страницы OLD {{ pagesOf(focusRegion, 'OLD') || '—' }} · NEW {{ pagesOf(focusRegion, 'NEW') || '—' }}</span>
    </div>
    <div v-if="layout === 'single'" class="sbm-tabs" role="tablist">
        <button v-for="side in panelSides" :key="side" type="button" role="tab" :aria-selected="String(narrowSide === side)"
                :class="{'is-active': narrowSide === side}" @click="narrowSide = side">{{ side }}</button>
    </div>
    <p v-if="layout === 'list'" class="sbm-context">Редактирование связей доступно на широком экране
        <a v-if="store.hmHref()" class="sbm-link" :href="store.hmHref()" target="_blank" rel="noopener">Открыть в Human Mapping ↗</a></p>
    <div v-if="layout !== 'list' && S.blocksActive && S.status" ref="canvas" class="sbm-canvas" :class="{'is-single': panelSides.length < 2 || layout === 'single'}">
        <section v-for="side in panelSides" :key="side" v-show="layout !== 'single' || narrowSide === side"
                 class="sbm-panel" :class="'sbm-panel--' + side.toLowerCase()">
            <header class="sbm-panel__head">
                <strong>{{ side }}</strong>
                <span class="sbm-panel__pages">
                    <button v-for="p in pages[side]" :key="p.page" type="button" class="sbm-link" @click="scrollToPage(side, p.page)">стр. {{ p.page }}</button>
                </span>
                <span class="sbm-panel__tools">
                    <button type="button" :class="{'is-active': panMode[side]}" :aria-pressed="String(panMode[side])" title="Переместить лист" @click="panMode[side] = !panMode[side]">✥</button>
                    <button type="button" :class="{'is-active': views[side].locked}" :aria-pressed="String(views[side].locked)" title="Зафиксировать положение" @click="lock(side)">🔒</button>
                    <button type="button" title="Сбросить положение" @click="reset(side)">↺</button>
                    <button type="button" title="Уменьшить" @click="zoom(side, -0.1)">−</button>
                    <span class="sbm-zoom">{{ Math.round(views[side].z * 100) }}%</span>
                    <button type="button" title="Увеличить" @click="zoom(side, 0.1)">+</button>
                </span>
            </header>
            <div class="sbm-scroll" :ref="el => setScroll(side, el)" :class="{'is-panning': panMode[side]}"
                 @scroll.passive="schedule" @pointerdown="panStart(side, $event)" @pointermove="panMove($event)"
                 @pointerup="panEnd" @pointercancel="panEnd">
                <div class="sbm-sheet" :style="sheetStyle(side)">
                    <article v-for="p in pages[side]" :key="p.page" class="sbm-page" :data-sbm-page="side + '-' + p.page">
                        <div class="sbm-page__label">{{ pageLabel(side, p) }}
                            <span v-if="!p.inPair" class="sbm-tag">вне пары</span><span v-else-if="S.lens.mode === 'region'" class="sbm-tag sbm-tag--in">в паре</span></div>
                        <div class="sbm-page__box" :style="{aspectRatio: ratio(side, p.page)}">
                            <img v-if="!failed[side + p.page] && store.rasterUrl(side, p.page)" class="sbm-page__img" :src="store.rasterUrl(side, p.page)" loading="lazy" decoding="async"
                                 draggable="false" alt="" @load="imageLoaded(side, p.page, $event)" @error="imageFailed(side, p.page)">
                            <p v-if="failed[side + p.page]" class="sbm-page__note">{{ T.FAIL3(p.page) }}</p>
                            <p v-if="store.pageText(side, p.page, store.pageView(side, p.page))" class="sbm-page__note">{{ store.pageText(side, p.page, store.pageView(side, p.page)) }}</p>
                            <button v-for="b in store.blocksFor(side, p.page)" :key="b.block_id" type="button" class="sbm-block"
                                    :class="blockClass(side, b)" :style="blockStyle(b)" :data-sbm-block="b.block_id" :data-sbm-side="side"
                                    :aria-label="blockAria(side, b, p.page)" :title="blockTitle(side, b)"
                                    @click="clickBlock(side, b, p.page)"><span class="sbm-block__label">{{ blockCaption(side, b) }}</span></button>
                        </div>
                    </article>
                    <p v-if="morePages(side).length" class="sbm-more-pages">⋯ ещё блоки {{ focus }}:
                        <button v-for="pg in morePages(side)" :key="pg" type="button" class="sbm-link" @click="store.addExtraPage(side, pg)">стр. {{ pg }} ▸</button></p>
                    <p v-if="!pages[side].length" class="sbm-muted sbm-pad">Страниц нет.</p>
                </div>
            </div>
        </section>
        <svg v-if="layout === 'full' || layout === 'compact'" class="sbm-lines" :width="svgSize.w" :height="svgSize.h"
             :viewBox="'0 0 ' + svgSize.w + ' ' + svgSize.h" aria-hidden="false">
            <g v-for="line in lines" :key="line.key" class="sbm-edge" :class="line.cls" @click="store.selectLink(line.key)">
                <title>{{ line.title }}</title>
                <path class="sbm-edge__hit" :d="line.hit"></path>
                <path class="sbm-edge__line" :d="line.d"></path>
                <text v-if="line.glyph" class="sbm-edge__glyph" :x="line.mid.x" :y="line.mid.y">{{ line.glyph }}</text>
                <g v-for="(port, i) in line.ports" :key="i" class="sbm-port" :class="'sbm-port--' + port.kind"
                   @click.stop="portClick(port)" @dblclick.stop="portDouble(line)">
                    <circle :cx="port.x" :cy="port.y" r="4"></circle>
                    <text v-if="port.kind === 'offlens'" :x="port.x + (port.side === 'NEW' ? 6 : -6)" :y="port.y - 6"
                          :text-anchor="port.side === 'NEW' ? 'start' : 'end'">{{ portLabel(port) }}</text>
                </g>
            </g>
        </svg>
    </div>
    <p v-if="edgesInfo.truncated" class="sbm-context">Связей в линзе: {{ edgesInfo.total }} — показаны только связи выбранных блоков.
        <button type="button" class="sbm-link" @click="S.showAllEdges = true">Показать все связи</button></p>
    <div v-if="model && (layout === 'single' || layout === 'list') && edgesInfo.edges.length" class="sbm-table-wrap">
        <table class="sbm-table"><tbody>
            <tr v-for="e in edgesInfo.edges" :key="e.key" :class="{'is-selected': S.selection.link === e.key}" @click="store.selectLink(e.key)">
                <td>OLD {{ blockKind('OLD', e.old_block_id) }} (стр. {{ e.oldPage ?? '—' }}) → NEW {{ blockKind('NEW', e.new_block_id) }} (стр. {{ e.newPage ?? '—' }})</td>
                <td>{{ store.edgeView(e).label }}</td></tr>
        </tbody></table>
    </div>
    <div class="sbm-bottom">
        <section class="sbm-inspector" aria-label="Инспектор">
            <template v-if="selectedEdge">
                <strong>{{ selectedEdgeView.title }}</strong>
                <p>Происхождение: {{ selectedEdgeView.origin }} · состояние: {{ selectedEdgeView.label }}</p>
                <p v-if="selectedEdge.links.some(l => store.contextMarked(l.link_id))" class="sbm-mark">{{ T.CTX_MARK }}</p>
                <p v-if="selectedEdge.state === 'FORBIDDEN'" class="sbm-muted">Запрещена только эта связь. Эти блоки могут быть связаны с другими блоками.</p>
            </template>
            <template v-else-if="S.selection.block">
                <strong>{{ S.selection.block.side }} · {{ blockKind(S.selection.block.side, S.selection.block.id) }} · {{ pageCaption(S.selection.block.side, S.selection.block.page) }}</strong>
                <p v-if="blockRegions(S.selection.block.side, S.selection.block.id).length" class="sbm-chips">Регионы:
                    <button v-for="id in blockRegions(S.selection.block.side, S.selection.block.id)" :key="id" type="button" class="sbm-region"
                            :class="{'is-focus': id === focus}" @click="store.focusRegion(id)">{{ id }}</button></p>
                <p v-else-if="model" class="sbm-muted">не входит ни в один регион — закрепить нельзя</p>
                <p v-if="model">Связи блока: {{ blockLinkCounts(S.selection.block.side, S.selection.block.id) }}</p>
                <template v-if="blockDetail">
                    <p v-if="blockDetail.status === 'loading'" class="sbm-muted">Загрузка распознанного текста…</p>
                    <p v-else-if="blockDetail.status === 'error'" class="sbm-muted">Распознанный текст недоступен.</p>
                    <template v-else>
                        <div v-for="(t, i) in blockTables" :key="i" class="sbm-table-wrap"><table class="sbm-table">
                            <tr v-for="(row, j) in t" :key="j"><td v-for="(cell, k) in row" :key="k">{{ cell }}</td></tr></table></div>
                        <pre v-if="!blockTables.length || blockDetail.data.modality !== 'TABLE'" class="sbm-pre">{{ blockDetail.data.structured_md || 'Распознанный текст отсутствует.' }}</pre>
                    </template>
                </template>
            </template>
            <p v-else class="sbm-muted">Выберите блок или связь, чтобы увидеть подробности.</p>
        </section>
        <section v-if="model" class="sbm-decision" aria-label="Решение по региону">
            <template v-if="focus">
                <strong>Решение по региону {{ focus }}</strong>
                <p>{{ store.currentDecision(focus) }}</p>
                <details class="sbm-journal">
                    <summary>Журнал региона ({{ journalRows.length }}) ▾</summary>
                    <p class="sbm-muted">{{ T.JOURNAL_HEAD }}</p>
                    <ol>
                        <li v-for="row in journalRows" :key="row.key" :class="{'is-superseded': row.superseded}">
                            {{ row.when }} · {{ row.what }} · {{ row.size }}<template v-if="row.comment"> · «{{ row.comment }}»</template>
                            <template v-if="row.superseded"> · {{ row.superseded }}</template>
                            <em v-if="row.foreign"> · {{ row.foreign }}</em><em v-if="row.mark" class="sbm-mark"> · {{ row.mark }}</em>
                        </li>
                    </ol>
                </details>
            </template>
            <p v-else class="sbm-muted">Выберите регион на полке, чтобы принять решение</p>
        </section>
        <section v-else-if="S.binding && (S.reviews || S.edits)" class="sbm-decision" aria-label="Журнал прогона">
            <details class="sbm-journal" open><summary>Журнал ({{ journalRows.length }})</summary>
                <p class="sbm-muted">{{ T.JOURNAL_HEAD }}</p>
                <ol><li v-for="row in journalRows" :key="row.key" :class="{'is-superseded': row.superseded}">
                    {{ row.when }} · {{ row.region }} · {{ row.what }} · {{ row.size }}<em v-if="row.foreign"> · {{ row.foreign }}</em></li></ol>
            </details>
        </section>
    </div>
    <p v-for="h in msgs.hints" :key="h.key" class="sbm-hint">ⓘ {{ h.text }}
        <template v-for="a in h.actions || []" :key="a.id"><button type="button" class="sbm-link" @click="act(a.id)">{{ a.label }}</button></template>
        <details v-if="h.details && h.details.length" class="sbm-more"><summary>Подробнее</summary><code v-for="(d, i) in h.details" :key="i">{{ d }}</code></details>
    </p>
    <footer v-if="msgs.footer.length" class="sbm-footer"><p v-for="f in msgs.footer" :key="f.key">{{ f.text }}</p></footer>
</section>`;

    function registerWorkspace(app) {
        app.component('semantic-block-workspace', {
            props: {store: {type: Object, required: true}, rows: {type: Array, default: () => []},
                currentRow: {type: Object, default: null}, mapBuilt: {type: Boolean, default: true},
                sheetEntry: {type: Function, default: null}},
            emits: ['open-row', 'launch'],
            setup(props, {emit}) {
                const {ref, reactive, computed, watch, onMounted, onBeforeUnmount} = root.Vue;
                const Core = root.HumanMappingCore;
                const store = props.store, S = store.state;
                const canvas = ref(null);
                const scrollEls = {OLD: null, NEW: null};
                const views = reactive({OLD: Core.createView(), NEW: Core.createView()});
                const panMode = reactive({OLD: false, NEW: false});
                const width = ref(root.innerWidth || 1440);
                const narrowSide = ref('OLD');
                const lines = ref([]);
                const svgSize = reactive({w: 0, h: 0});
                const navOpen = ref(false), navFilter = ref('all');
                const failed = reactive({}), natural = reactive({});
                const layout = computed(() => layoutFor(width.value));
                const sheetOf = (side, page) => {
                    const entry = props.sheetEntry ? props.sheetEntry(VIEWER_SIDE[side], page) : null;
                    return entry && entry.sheet_number ? String(entry.sheet_number) : '';
                };
                const model = computed(() => store.model());
                const focus = computed(() => { void S.version; void S.lens; return store.focusId(); });
                const focusRegion = computed(() => store.regionById(focus.value));
                const msgs = computed(() => store.messages({rows: props.rows, mapBuilt: props.mapBuilt, sheetOf}));
                const pages = computed(() => { void S.lens; void S.version; return store.lensPages(); });
                const panelSides = computed(() => {
                    const row = S.lens.row;
                    if (S.lens.mode === 'pair' && row && !Core.isTwoSided(row)) return [row.leftPages.length ? 'OLD' : 'NEW'];
                    return SIDES;
                });
                const edgesInfo = computed(() => { void S.version; void S.lens; void S.selection; void S.focusOnly; void S.showAllEdges; return store.lensEdges(); });
                const twoSided = computed(() => props.rows.filter(Core.isTwoSided));
                const pairIndex = computed(() => twoSided.value.findIndex(r => Core.groupKey(r.leftPages, r.rightPages) === S.lens.key));
                const bindingOptions = computed(() => { void S.status; void S.binding; return store.bindingOptions(); });
                const shelfData = computed(() => { void S.version; void S.lens; return store.shelves(); });
                const shelfList = computed(() => [{id: 'both', label: 'Обе стороны', items: shelfData.value.both},
                    {id: 'old', label: 'Только OLD', items: shelfData.value.old}, {id: 'new', label: 'Только NEW', items: shelfData.value.new}]);
                const shelfRegions = computed(() => [...shelfData.value.both, ...shelfData.value.old, ...shelfData.value.new]);
                const untouchedIds = computed(() => new Set(store.untouched(props.rows).map(r => r.id)));
                const unpairedIds = computed(() => new Set(store.unpaired(props.rows).map(r => r.id)));
                const navFilters = computed(() => {
                    const m = model.value;
                    if (!m) return [];
                    const row = S.lens.row;
                    const touching = row ? m.regions.filter(r => Core.rel(r, row) !== 'NONE').length : 0;
                    return [{id: 'all', label: `Все (${m.regions.length})`},
                        {id: 'row', label: `Касаются этой пары (${touching})`},
                        {id: 'untouched', label: `Вне карты листов (${untouchedIds.value.size})`},
                        {id: 'unpaired', label: T.UNPAIRED(unpairedIds.value.size), hint: T.UNPAIRED_HINT},
                        {id: 'decided', label: 'С решением'}, {id: 'attention', label: 'Требуют внимания'}];
                });
                const navList = computed(() => {
                    const m = model.value;
                    if (!m) return [];
                    const row = S.lens.row, f = navFilter.value;
                    return m.regions.filter(r => f === 'all' || (f === 'row' && row && Core.rel(r, row) !== 'NONE')
                        || (f === 'untouched' && untouchedIds.value.has(r.id)) || (f === 'unpaired' && unpairedIds.value.has(r.id))
                        || (f === 'decided' && m.info.get(r.id).status !== 'UNREVIEWED') || (f === 'attention' && m.info.get(r.id).attention));
                });
                const journalRows = computed(() => { void S.version; return store.journal(model.value ? focus.value : ''); });
                const selectedEdge = computed(() => { const m = model.value; return m && S.selection.link ? m.edges.get(S.selection.link) || null : null; });
                const selectedEdgeView = computed(() => (selectedEdge.value ? store.edgeView(selectedEdge.value) : null));
                const blockDetail = computed(() => { void S.details; const b = S.selection.block; return b ? store.detail(b.side, b.id) : null; });
                const blockTables = computed(() => {
                    const d = blockDetail.value;
                    return d && d.status === 'ok' ? (d.data.tables || []).map(parseTable).filter(Boolean) : [];
                });

                function choose(opt, event) {
                    const details = event && event.target && event.target.closest ? event.target.closest('details') : null;
                    if (details) details.open = false;
                    store.bind(opt.kind === 'SNAP' ? {kind: 'SNAP', resultId: opt.id} : {kind: 'LIVE', runId: opt.id});
                }
                function act(id) {
                    if (id === 'retry') store.retry();
                    else if (id === 'go-new') store.bindCurrent();
                    else if (id === 'stay') store.dismissNewRun();
                    else if (id === 'show-conflict') store.showConflict();
                    else if (id === 'launch') emit('launch');
                    else if (id === 'choose') {
                        const el = canvas.value && canvas.value.closest ? canvas.value.closest('.sbm-workspace') : null;
                        const details = el ? el.querySelector('.sbm-binding') : (root.document && root.document.querySelector('.sbm-binding'));
                        if (details) details.open = true;
                    }
                }
                function stepPair(delta) {
                    const next = twoSided.value[pairIndex.value + delta];
                    if (next) emit('open-row', next);
                }
                function openRegion(id) {
                    navOpen.value = false;
                    store.openRegion(id, false);
                }
                const attention = id => !!(model.value && (model.value.info.get(id) || {}).attention);
                const pagesOf = (r, side) => Core.regionPages(r, side).join(', ');
                const cardinality = r => CARDINALITY[r.member_cardinality] || r.member_cardinality;
                function regionChip(r) {
                    const stack = pages.value;
                    const rest = side => Core.regionPages(r, side).filter(p => !stack[side].some(x => x.page === p));
                    const more = [rest('OLD').length ? 'OLD ' + rest('OLD').join(', ') : '', rest('NEW').length ? 'NEW ' + rest('NEW').join(', ') : '']
                        .filter(Boolean).join(' · ');
                    const off = edgesInfo.value.offLens.get(r.id);
                    return [(r.id === focus.value ? '◆ ' : '') + r.id + ' ' + clip(r.domain, 34), more ? 'ещё стр. ' + more : '',
                        store.regionStatusLabel(r.id), off ? `связей вне этих листов: ${off} ▸` : ''].filter(Boolean).join(' · ');
                }
                function regionTitle(r, shelf) {
                    const other = shelf === 'old' ? 'NEW' : shelf === 'new' ? 'OLD' : '';
                    const hint = other ? ` · ${other}-страницы региона: ${pagesOf(r, other) || '—'} — не в этой паре` : '';
                    return r.domain + hint;
                }
                const navLabel = r => [r.id, clip(r.domain, 40), cardinality(r), 'OLD ' + (pagesOf(r, 'OLD') || '—'),
                    'NEW ' + (pagesOf(r, 'NEW') || '—'), store.regionStatusLabel(r.id)].join(' · ');
                function morePages(side) {
                    const r = focusRegion.value;
                    if (!r || S.lens.mode !== 'pair') return [];
                    const members = (side === 'OLD' ? r.old_blocks : r.new_blocks).map(b => Number(b.page));
                    return uniqSorted(members).filter(p => !pages.value[side].some(x => x.page === p));
                }
                function pageLabel(side, p) {
                    const sheet = sheetOf(side, p.page);
                    return (sheet ? `лист ${sheet} · ` : '') + `стр. ${p.page}`;
                }
                const pageCaption = (side, page) => (page ? pageLabel(side, {page}) : 'страница не указана');
                function ratio(side, page) {
                    const view = store.pageView(side, page);
                    const g = view.geometry;
                    if (g && g.width_px && g.height_px) return String(g.width_px / g.height_px);
                    return String(natural[side + page] || DEFAULT_RATIO);
                }
                function imageLoaded(side, page, event) {
                    const img = event.target;
                    const view = store.pageView(side, page);
                    if (!(view.geometry && view.geometry.width_px) && img.naturalWidth && img.naturalHeight) {
                        natural[side + page] = img.naturalWidth / img.naturalHeight;
                    }
                    schedule();
                }
                function imageFailed(side, page) { failed[side + page] = true; }
                const regionsOf = (side, id) => (model.value ? model.value.blockRegions.get(side + '|' + id) || [] : []);
                const blockRegions = (side, id) => regionsOf(side, id);
                function blockClass(side, b) {
                    const m = model.value, regs = regionsOf(side, b.block_id);
                    const sel = S.selection, link = selectedEdge.value;
                    return ['sbm-block--' + String(b.modality || 'text').toLowerCase(), {
                        'is-source': !m,
                        'is-stamp': b.source_block_type === 'stamp',
                        'is-member-focus': !!m && regs.includes(focus.value),
                        'is-member': !!m && regs.length && !regs.includes(focus.value),
                        'is-outside': !!m && !regs.length,
                        'is-selected': sel[side].includes(b.block_id),
                        'is-link-end': !!link && (side === 'OLD' ? link.old_block_id : link.new_block_id) === b.block_id,
                    }];
                }
                function blockStyle(b) {
                    const p = Core.bboxPercent(b.bbox);
                    return {left: p.left + '%', top: p.top + '%', width: p.width + '%', height: p.height + '%'};
                }
                function blockCaption(side, b) {
                    const regs = regionsOf(side, b.block_id);
                    const kind = b.source_block_type === 'stamp' ? 'штамп' : modality(b.modality);
                    if (!regs.length) return kind;
                    const lead = regs.includes(focus.value) ? focus.value : regs[0];
                    return kind + ' · ' + lead + (regs.length > 1 ? ` · ещё в ${regs.length - 1} ${plural(regs.length - 1, 'регионе', 'регионах', 'регионах')}` : '');
                }
                function blockTitle(side, b) {
                    const regs = regionsOf(side, b.block_id);
                    if (model.value && !regs.length) return 'не входит ни в один регион — закрепить нельзя';
                    return regs.length ? regs.join(', ') : modality(b.modality);
                }
                const blockAria = (side, b, page) => [side, b.source_block_type === 'stamp' ? 'штамп' : modality(b.modality),
                    'стр. ' + page, regionsOf(side, b.block_id).length ? 'регион ' + regionsOf(side, b.block_id).join(', ') : ''].filter(Boolean).join(', ');
                function blockKind(side, id) {
                    const m = model.value;
                    const known = m && m.blockType.get(side + '|' + id);
                    return known ? modality(known.type) : 'блок';
                }
                function blockLinkCounts(side, id) {
                    const m = model.value;
                    if (!m) return '';
                    const mine = [...m.edges.values()].filter(e => (side === 'OLD' ? e.old_block_id : e.new_block_id) === id);
                    const a = mine.filter(e => e.links.some(l => l.state === 'ANCHOR')).length;
                    const f = mine.filter(e => e.links.some(l => l.state === 'FORBIDDEN')).length;
                    return `закреплено: ${a} · запрещено: ${f} · без ограничений: ${mine.length - a - f}`;
                }
                function clickBlock(side, b, page) {
                    if (panMode[side]) return;
                    store.selectBlock(side, b.block_id, page);
                }
                function portLabel(port) {
                    return port.single ? `⇥ ${port.side} стр. ${port.page ?? '—'}` : `⇥ стр. ${port.page ?? '—'} (вне пары)`;
                }
                function portClick(port) {
                    if (port.kind === 'offlens' && port.page) store.addExtraPage(port.side, port.page);
                }
                function portDouble(line) {
                    const m = model.value, edge = m && m.edges.get(line.key);
                    if (edge) store.focusRegion(edge.regionIds.includes(focus.value) ? focus.value : edge.regionIds[0], 'region');
                }

                // ── pan / zoom / lock / reset (independent per side, HM rules via the core) ──
                let drag = null;
                function panStart(side, event) {
                    if (views[side].locked) return;
                    const onBlock = event.target && event.target.closest && event.target.closest('[data-sbm-block]');
                    if (onBlock && !panMode[side]) return;
                    drag = {side, x: event.clientX, y: event.clientY, ox: views[side].x, oy: views[side].y};
                    try { event.currentTarget.setPointerCapture(event.pointerId); } catch (_) { /* optional */ }
                }
                function panMove(event) {
                    if (!drag) return;
                    const v = views[drag.side];
                    views[drag.side] = Core.panView({...v, x: drag.ox, y: drag.oy}, event.clientX - drag.x, event.clientY - drag.y);
                    schedule();
                }
                function panEnd() { drag = null; }
                function zoom(side, delta) { views[side] = Core.zoomView(views[side], Math.round((views[side].z + delta) * 10) / 10); schedule(); }
                function reset(side) { views[side] = Core.resetView(views[side]); schedule(); }
                function lock(side) { views[side] = Core.lockView(views[side], !views[side].locked); }
                const sheetStyle = side => ({width: views[side].z * 100 + '%', transform: `translate(${views[side].x}px, ${views[side].y}px)`});
                function setScroll(side, el) { scrollEls[side] = el; if (el && observer) observer.observe(el); }
                function scrollToPage(side, page) {
                    const el = scrollEls[side];
                    const target = el && el.querySelector(`[data-sbm-page="${side}-${page}"]`);
                    if (target) el.scrollTop = target.offsetTop - 8;
                }

                // ── lines: one SVG over the canvas, redrawn in an animation frame ──
                let frame = 0;
                function schedule() {
                    if (frame || typeof root.requestAnimationFrame !== 'function') return;
                    frame = root.requestAnimationFrame(() => { frame = 0; redraw(); });
                }
                function redraw() {
                    const c = canvas.value;
                    if (!c || !['full', 'compact'].includes(layout.value)) { lines.value = []; return; }
                    const origin = c.getBoundingClientRect();
                    svgSize.w = Math.round(origin.width);
                    svgSize.h = Math.round(origin.height);
                    const rel = r => ({left: r.left - origin.left, top: r.top - origin.top, width: r.width, height: r.height});
                    const rects = {OLD: null, NEW: null};
                    for (const side of panelSides.value) if (scrollEls[side]) rects[side] = rel(scrollEls[side].getBoundingClientRect());
                    const centers = {OLD: new Map(), NEW: new Map()};
                    c.querySelectorAll('[data-sbm-block]').forEach(el => {
                        const r = el.getBoundingClientRect();
                        centers[el.dataset.sbmSide].set(el.dataset.sbmBlock, {x: r.left + r.width / 2 - origin.left, y: r.top + r.height / 2 - origin.top});
                    });
                    const edges = edgesInfo.value.edges;
                    const byKey = new Map(edges.map(e => [e.key, e]));
                    lines.value = computeLinkGeometry({edges, centers, rects}).map(g => {
                        const edge = byKey.get(g.key), view = store.edgeView(edge);
                        return {...g, title: view.title, glyph: view.glyph,
                            cls: ['sbm-edge--' + view.state.toLowerCase(), 'sbm-edge--' + view.kind,
                                {'is-dim': !edge.focused, 'is-selected': S.selection.link === g.key}]};
                    });
                }
                let observer = null;
                const onResize = () => { width.value = root.innerWidth || width.value; schedule(); };
                const onFocus = () => store.refreshStatus();
                onMounted(() => {
                    if (typeof root.ResizeObserver === 'function') {
                        observer = new root.ResizeObserver(schedule);
                        if (canvas.value) observer.observe(canvas.value);
                        for (const side of SIDES) if (scrollEls[side]) observer.observe(scrollEls[side]);
                    }
                    if (root.addEventListener) { root.addEventListener('resize', onResize); root.addEventListener('focus', onFocus); }
                    if (!S.lens.row && props.currentRow) store.openRow(props.currentRow);
                    else if (!S.lens.row && !props.rows.length && model.value && !S.lens.focus) {
                        const first = model.value.regions.find(r => model.value.info.get(r.id).status === 'UNREVIEWED') || model.value.regions[0];
                        if (first) store.openRegion(first.id, false);
                    }
                    schedule();
                });
                onBeforeUnmount(() => {
                    if (observer) observer.disconnect();
                    if (root.removeEventListener) { root.removeEventListener('resize', onResize); root.removeEventListener('focus', onFocus); }
                    if (frame && root.cancelAnimationFrame) root.cancelAnimationFrame(frame);
                });
                watch(() => [S.version, S.lens, S.selection, S.pages, S.geometry, S.showStamps, S.focusOnly, S.showAllEdges,
                    layout.value, narrowSide.value], schedule, {flush: 'post'});
                watch(views, schedule, {deep: true, flush: 'post'});
                // Lens follows the sheet map: same composition stays; a click on another row rebuilds it.
                const sig = rows => rows.map(r => Core.groupKey(r.leftPages, r.rightPages)).join(';');
                const keyOf = row => (row ? Core.groupKey(row.leftPages, row.rightPages) : '');
                let lastRows = sig(props.rows), lastCurrent = keyOf(props.currentRow);
                watch(() => [sig(props.rows), keyOf(props.currentRow)], ([rowsSig, current]) => {
                    const rowsChanged = rowsSig !== lastRows, currentChanged = current !== lastCurrent;
                    lastRows = rowsSig;
                    lastCurrent = current;
                    if (rowsChanged) { store.syncRows(props.rows); return; }
                    if (currentChanged && current && current !== S.lens.key) store.openRow(props.currentRow);
                });
                watch(model, m => {
                    if (m && !S.lens.row && !props.rows.length && !S.lens.focus) {
                        const first = m.regions.find(r => m.info.get(r.id).status === 'UNREVIEWED') || m.regions[0];
                        if (first) store.openRegion(first.id, false);
                    }
                });

                return {S, T, store, canvas, views, panMode, layout, narrowSide, lines, svgSize, navOpen, navFilter, failed,
                    model, focus, focusRegion, msgs, pages, panelSides, edgesInfo, twoSided, pairIndex, bindingOptions,
                    shelfList, shelfRegions, navFilters, navList, journalRows, selectedEdge, selectedEdgeView, blockDetail,
                    blockTables, choose, act, stepPair, openRegion, attention, pagesOf, cardinality, regionChip, regionTitle,
                    navLabel, morePages, pageLabel, pageCaption, ratio, imageLoaded, imageFailed, blockRegions, blockClass,
                    blockStyle, blockCaption, blockTitle, blockAria, blockKind, blockLinkCounts, clickBlock, portLabel,
                    portClick, portDouble, panStart, panMove, panEnd, zoom, reset, lock, sheetStyle, setScroll,
                    scrollToPage, schedule, clip};
            },
            template: WORKSPACE_TEMPLATE,
        });
    }

    function registerMapExtras(app) {
        // Lines under the sheet-map head: the sheet layer (explicit formulas, UNIFIED §9, not
        // pcReviewSheetCount) and, with a binding, the semantic layer of that run.
        app.component('sbm-sheet-summary', {
            props: {store: {type: Object, required: true}, rows: {type: Array, default: () => []},
                matchState: {type: Object, default: null}, saving: Boolean, statusOf: {type: Function, default: null}},
            setup(props) {
                const {computed} = root.Vue;
                const S = props.store.state;
                const sheets = computed(() => {
                    const links = (props.matchState && props.matchState.links && props.matchState.links.links) || [];
                    const review = props.rows.filter(r => (props.statusOf ? props.statusOf(r).tone === 'review'
                        : root.HumanMappingCore.isTwoSided(r) && r.confidence !== 'high' && r.source !== 'manual')).length;
                    const oldOnly = props.rows.filter(r => r.leftPages.length && !r.rightPages.length).length;
                    const newOnly = props.rows.filter(r => r.rightPages.length && !r.leftPages.length).length;
                    const review2 = root.StageComparisonReview;
                    const done = !!(review2 && review2.comparisonLaunchDecisionsComplete
                        && review2.comparisonLaunchDecisionsComplete(props.matchState));
                    const updated = props.matchState && props.matchState.links && props.matchState.links.updated_at;
                    const saved = props.saving ? 'сохраняю…' : updated ? 'сохранено автоматически ' + clock(updated) : '';
                    return [`Листы: сопоставлено ${links.length}`, `«Проверить» ${review}`, `без пары: OLD ${oldOnly}, NEW ${newOnly}`,
                        done ? T.SHEETS_DONE : '', saved].filter(Boolean).join(' · ');
                });
                const semantic = computed(() => { void S.version; return props.store.summary(); });
                const offMap = computed(() => {
                    void S.version;
                    return props.store.offMapCount((props.matchState && props.matchState.links && props.matchState.links.links) || []);
                });
                return {sheets, semantic, offMap};
            },
            template: `<div class="sbm-sheet-summary">
                <span class="sbm-sheet-summary__line">{{ sheets }}</span>
                <span v-if="semantic" class="sbm-sheet-summary__line">{{ semantic.text }}<template v-if="offMap"> · Вне сопоставленных листов: {{ offMap }}</template></span>
            </div>`,
        });
        // Under the sheet-map list (not a .sc-sheet-map__row): regions outside every two-sided row
        // (untouchedRegions, C3) and the one-time placement notice of MASTER §12.
        app.component('sbm-map-footer', {
            props: {store: {type: Object, required: true}, rows: {type: Array, default: () => []}},
            emits: ['open'],
            setup(props, {emit}) {
                const {computed} = root.Vue;
                const S = props.store.state;
                const untouched = computed(() => { void S.version; return props.store.untouched(props.rows); });
                function open(id) { props.store.openRegion(id); emit('open'); }
                function show() { props.store.showNoticeTarget(); emit('open'); }
                return {S, T, untouched, open, show};
            },
            template: `<div class="sbm-map-footer">
                <div v-if="S.notice" class="sbm-map-notice" role="status"><span>{{ S.notice.text }}</span>
                    <button type="button" class="sbm-link" @click="show">Показать</button>
                    <button type="button" class="sbm-link" aria-label="Скрыть уведомление" @click="store.dismissNotice()">×</button></div>
                <div v-if="untouched.length" class="sbm-untouched">
                    <span>{{ T.UNTOUCHED(untouched.length) }} —</span>
                    <template v-for="(r, i) in untouched" :key="r.id"><span v-if="i"> · </span><button type="button" class="sbm-link" :title="r.domain" @click="open(r.id)">{{ r.id }}</button></template>
                    <span aria-hidden="true"> ▸</span>
                </div>
            </div>`,
        });
    }

    root.StageBlockMapping = {
        createStore,
        register(app) {
            if (!root.HumanMappingCore) return;
            registerWorkspace(app);
            registerMapExtras(app);
        },
        // Pure helpers, exported for tests.
        T, chooseBinding, bindingKey, buildModel, fromIndexRegion, fromUiRegion, rowChipOf, summaryOf,
        placementNotice, visibleBlocks, computeLinkGeometry, parseTable, previewUrl, hmAssetUrl, humanMappingHref,
        errorCode, createClient, layoutFor, plural, dayMonth, dayMonthTime, clock,
    };
}(typeof globalThis !== 'undefined' ? globalThis : this));
