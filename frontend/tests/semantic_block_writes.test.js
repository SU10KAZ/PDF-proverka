// Phase C: writing from stage 2 — the same Human Mapping events through the same API as the classic page.
// The classic page's own functions (shim, addLinks, deleteLink, reassignLink, save) are cut out of the
// served HTML_PAGE and run in node:vm next to the store; requests must be equal byte for byte.
// Fake fetch only: no network, no model.
import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8');
const coreSource = read('../static/js/human-mapping-core.js');
const source = read('../static/js/stage-block-mapping.js');
const vueSource = read('../static/js/vue.global.prod.js');
const routerSource = read('../../backend/app/api/routers/human_mapping.py');

const SID = 'e6fc8a2725eb4a67', PID = 'p290a06df79', OID = '4f3e5916';
const RUN = 'a631b49aaaac4db0af66a495c155c629';
const SNAP = 'pcv3snap_5157938d548fb1df6fbcf87c200c1347';
const HM = `/api/human-mapping/objects/${OID}/comparisons/${PID}`;
const SCOPE = `?session_id=${SID}&run_id=${RUN}`;

// Region R (N↔N): OLD o1, o2 on page 20 · NEW n1, n2 on page 10. Region Q shares o1 and n1.
function region(id, olds, news) {
    const m = {OLD: olds.map(([bid, page]) => ({id: bid, page, type: 'TEXT'})), NEW: news.map(([bid, page]) => ({id: bid, page, type: 'TABLE'}))};
    return {id, title: id, domain: id + ' домен', scope: '', membership_state: {OLD: 'MAPPED', NEW: 'MAPPED'}, mapping_state: 'MAPPED',
        member_cardinality: 'N:N', pages: {OLD: [...new Set(olds.map(x => x[1]))], NEW: [...new Set(news.map(x => x[1]))]}, members: m,
        allowed: {OLD: m.OLD.map(b => b.id), NEW: m.NEW.map(b => b.id)}, invalid_ref_count: 0};
}
const INDEX = {schema: 'stage-block-mapping-region-index/1', regions: [
    region('R', [['o1', 20], ['o2', 20]], [['n1', 10], ['n2', 10]]),
    region('Q', [['o1', 20], ['o3', 21]], [['n1', 10], ['n3', 11]]),
]};
const PAGE_R = {id: 'R', old_blocks: [{id: 'o1'}, {id: 'o2'}], new_blocks: [{id: 'n1'}, {id: 'n2'}], pages: {OLD: [], NEW: []}};
const ROW = {key: 'explicit-0', leftPages: [20], rightPages: [10], source: 'auto', confidence: 'high', explicitLinkIndex: 0};

function runEntry(extra = {}) {
    return {run_id: RUN, state: 'COMPLETED_FROZEN', frozen: true, is_current: true, created_at: '2026-09-21T10:00:00+00:00',
        completed_at: '2026-09-21T12:00:00+00:00', engine_version: '3.5.2', model: 'claude-opus-5', model_display: 'Claude Opus 5',
        projectchange_count: 64, region_count: 2, hm_available: true, hm_reason: null, pdf_match: true, source_stale: false,
        blocks_content_match: null, history: {reviews: 0, block_link_events: 0, poisoned: false, poison_codes: []},
        writable: true, write_block_reason: null, write_block_reasons: [], ...extra};
}
function statusBody({writes = true, run = {}, snapshots = []} = {}) {
    return {schema: 'stage-block-mapping-status/1', session_id: SID, pair_id: PID, object_id: OID, object_error: null,
        current_run_id: RUN, latest_attempt: null, runs: [runEntry(writes ? run : {writable: false,
            write_block_reason: 'WRITES_DISABLED', write_block_reasons: ['WRITES_DISABLED'], ...run})],
        snapshots, source_blocks: {OLD: {pdf_sha256: 'a'.repeat(64)}, NEW: {pdf_sha256: 'b'.repeat(64)}},
        capabilities: {block_mapping_writes: writes}};
}
const uuids = () => { let n = 0; return () => `aaaaaaaa-0000-4000-8000-${String(++n).padStart(12, '0')}`; };
const at = n => `2026-09-24T11:${String(n).padStart(2, '0')}:00.000000+00:00`;
const hmRow = extra => ({object_id: OID, pair_key: PID, comparison_id: PID, pair: PID, run_id: RUN, result_id: null, comment: '', ...extra});
const addEvent = (event_id, link_id, o, n, t, region_id = 'R') => hmRow({event_id, event_type: 'ADD_BLOCK_LINK', region_id, link_id,
    old_block_id: o, new_block_id: n, previous_old_block_id: null, previous_new_block_id: null, previous_link_id: null, timestamp: at(t)});
const reviewRow = (review_id, status, olds, news, t, region_id = 'R') => hmRow({review_id, region_id, status, old_block_ids: olds,
    new_block_ids: news, timestamp: at(t), reviewer_source: 'HUMAN'});

// A fake portal: GETs from the current history, POSTs appended like the HM router (or answered by `answer`).
function portal({status = statusBody(), reviews = [], edits = [], answer = null, hold = false} = {}) {
    const calls = [], waiting = [];
    const server = {reviews: [...reviews], edits: [...edits]};
    let clock = 50;
    const reply = (body, ok = true, code = 200) => ({ok, status: code, json: async () => body});
    const store = body => {
        const row = {...hmRow({timestamp: at(++clock)}), ...body};
        if ('status' in body) { row.review_id = 'rv' + clock; server.reviews.push(row); } else { row.event_id = 'ev' + clock; server.edits.push(row); }
        return row;
    };
    const fetch = (url, init = {}) => {
        const call = {url, method: init.method || 'GET', headers: init.headers, body: init.body};
        calls.push(call);
        const path = new URL(url, 'http://portal.test').pathname;
        if (call.method === 'POST') {
            const respond = () => {
                const custom = answer && answer(call, calls.filter(c => c.method === 'POST').length);
                if (custom === 'reject') return Promise.reject(new TypeError('network'));
                if (custom) return Promise.resolve(reply(custom.body, custom.code < 400, custom.code));
                return Promise.resolve(reply(store(JSON.parse(init.body))));
            };
            if (!hold) return respond();
            return new Promise(resolve => waiting.push(() => resolve(respond())));
        }
        if (path.endsWith('/status')) return Promise.resolve(reply(status));
        if (path.endsWith('/region-index')) return Promise.resolve(reply(INDEX));
        if (path.endsWith('/reviews')) return Promise.resolve(reply([...server.reviews]));
        if (path.endsWith('/block-links')) return Promise.resolve(reply([...server.edits]));
        return Promise.resolve(reply({detail: 'not found'}, false, 404));
    };
    const sandbox = {console, setTimeout, URLSearchParams, Uint8Array};
    sandbox.globalThis = sandbox;
    vm.runInNewContext(coreSource, sandbox);
    vm.runInNewContext(source, sandbox);
    const s = sandbox.StageBlockMapping.createStore({fetch, Vue: null, sessionStorage: null, uuid: uuids()});
    return {store: s, S: s.state, calls, server, waiting, SBM: sandbox.StageBlockMapping,
        posts: () => calls.filter(c => c.method === 'POST')};
}
async function ready(options) {
    const env = portal(options);
    await env.store.load({sessionId: SID, pairId: PID});
    env.store.openRow(ROW);
    return env;
}
const select = (env, olds, news) => { env.S.selection = {OLD: olds, NEW: news, link: '', block: null}; };
const flush = () => new Promise(resolve => setTimeout(resolve, 0));

// The classic page, as served: its fetch shim and its own write functions, cut out of HTML_PAGE.
function servedPage() {
    const start = routerSource.indexOf("HTML_PAGE = r'''") + "HTML_PAGE = r'''".length;
    return routerSource.slice(start, routerSource.indexOf("'''", start));
}
function classicPage({edits = [], reviews = [], selected = {OLD: [], NEW: []}, selectedLinkId = null, comment = ''} = {}) {
    const page = servedPage();
    const shim = page.slice(page.indexOf('(function(){'), page.indexOf('})();', page.indexOf('(function(){')) + 5)
        .replace('__HM_CONTEXT_JSON__', JSON.stringify({run_id: RUN, result_id: null, object: OID, pair: PID, fixture_letter: null,
            fixture_nav: false, label: PID, data_source: 'PUBLISHED', session_id: SID, seed: null}));
    const line = prefix => {
        const found = page.split('\n').find(l => l.startsWith(prefix));
        if (!found) throw new Error('HM page no longer has ' + prefix);
        return found;
    };
    const requests = [];
    const next = uuids();
    const sandbox = {console, alert: () => {}, confirm: () => true, crypto: {randomUUID: next}};
    sandbox.window = sandbox;
    sandbox.fetch = (url, init) => {
        requests.push({url, method: init.method, headers: init.headers, body: init.body});
        const body = JSON.parse(init.body);
        const row = 'status' in body ? {...body, review_id: 'rv'} : {...body, event_id: 'ev', timestamp: at(59)};
        return Promise.resolve({ok: true, status: 200, json: async () => row});
    };
    vm.runInNewContext(shim, sandbox);
    const script = [
        `var data={pair_key:${JSON.stringify(PID)}},editMode=true,reassignSide=null;`,
        `var selected=${JSON.stringify(selected)},selectedLinkId=${JSON.stringify(selectedLinkId)};`,
        `var linkEvents=${JSON.stringify(edits)},history=${JSON.stringify(reviews)};`,
        `const $=s=>s==='#comment'?{value:${JSON.stringify(comment)}}:null;function render(){}`,
        ...['function latest(r)', 'function baseProposedLinks(r)', 'function effectiveLinks(r)', 'async function saveLinkEvent(',
            'async function addLinks(', 'async function deleteLink(', 'async function reassignLink(', 'async function save('].map(line),
    ].join('\n');
    vm.runInNewContext(script, sandbox);
    return {page: sandbox, requests};
}
const strip = r => ({url: r.url, method: r.method, headers: r.headers, body: r.body});

describe('requests are byte-for-byte those of the classic Human Mapping page', () => {
    it('1→1 link: ADD_BLOCK_LINK with the explicit scope', async () => {
        const hm = classicPage({selected: {OLD: ['o1'], NEW: ['n2']}, comment: 'проверено'});
        await hm.page.addLinks(PAGE_R, 'one');
        const env = await ready();
        select(env, ['o1'], ['n2']);
        env.store.setComment('проверено');
        const res = await env.store.connect('one');
        expect(res.ok).toBe(true);
        expect(env.posts().map(strip)).toEqual(hm.requests.map(strip));
        expect(hm.requests[0].url).toBe(`${HM}/block-links${SCOPE}`);
        expect(hm.requests[0].body).toBe(JSON.stringify({event_type: 'ADD_BLOCK_LINK', pair_key: PID, region_id: 'R',
            link_id: 'human:aaaaaaaa-0000-4000-8000-000000000001', old_block_id: 'o1', new_block_id: 'n2',
            previous_old_block_id: null, previous_new_block_id: null, comment: 'проверено'}));
    });

    it('fan 1→N and explicit N×N (after the count is confirmed), existing edges skipped', async () => {
        const edits = [addEvent('e0', 'human:x', 'o1', 'n1', 1)];
        for (const [mode, olds, news] of [['spoke', ['o1'], ['n1', 'n2']], ['cartesian', ['o1', 'o2'], ['n1', 'n2']]]) {
            const hm = classicPage({edits, selected: {OLD: olds, NEW: news}});
            await hm.page.addLinks(PAGE_R, mode);
            const env = await ready({edits});
            select(env, olds, news);
            const first = await env.store.connect(mode);
            if (mode === 'cartesian') {
                expect(first.pending).toBe('confirm');
                // o1→n1 already exists: the count is what will be created.
                expect(env.S.edit.dialog.text).toBe('Будет создано связей: 3 (все пары выбранных блоков OLD × NEW). Продолжить?');
                expect(env.posts()).toEqual([]);
                await env.store.commitDialog();
            }
            expect(hm.requests, mode).toHaveLength(mode === 'spoke' ? 1 : 3);
            expect(env.posts().map(strip), mode).toEqual(hm.requests.map(strip));
        }
    });

    it('delete and reassign of the selected link (new human:<uuid>, previous_link_id)', async () => {
        const edits = [addEvent('e0', 'human:x', 'o1', 'n1', 1)];
        let hm = classicPage({edits, selectedLinkId: 'human:x', comment: 'не то'});
        await hm.page.deleteLink(PAGE_R);
        let env = await ready({edits});
        env.store.selectLink('o1|n1');
        env.store.setComment('не то');
        await env.store.deleteLink();
        expect(env.posts().map(strip)).toEqual(hm.requests.map(strip));
        expect(JSON.parse(hm.requests[0].body).event_type).toBe('DELETE_BLOCK_LINK');

        hm = classicPage({edits, selectedLinkId: 'human:x'});
        await hm.page.reassignLink(PAGE_R, 'NEW', 'n2');
        env = await ready({edits});
        env.store.selectLink('o1|n1');
        env.store.startReassign('NEW');
        await env.store.reassignTo('NEW', 'n2');
        expect(env.posts().map(strip)).toEqual(hm.requests.map(strip));
        expect(JSON.parse(hm.requests[0].body)).toMatchObject({event_type: 'REASSIGN_BLOCK_LINK',
            link_id: 'human:aaaaaaaa-0000-4000-8000-000000000001', previous_link_id: 'human:x'});
    });

    it('region decision: the selection, or all members when nothing is selected; previous_review_id', async () => {
        const reviews = [reviewRow('r0', 'HUMAN_UNCERTAIN', ['o1'], ['n1'], 2)];
        for (const [status, selected] of [['HUMAN_CONFIRMED', {OLD: ['o2'], NEW: ['n2']}], ['HUMAN_REJECTED', {OLD: [], NEW: []}]]) {
            const hm = classicPage({reviews, selected, comment: 'итог'});
            await hm.page.save(PAGE_R, status);
            const env = await ready({reviews});
            select(env, selected.OLD, selected.NEW);
            env.store.focusRegion('R');
            env.store.setComment('итог');
            expect((await env.store.regionDecision(status)).pending).toBe('preview');
            expect(env.posts()).toEqual([]);
            await env.store.commitDialog();
            expect(env.posts().map(strip), status).toEqual(hm.requests.map(strip));
            expect(hm.requests[0].url).toBe(`${HM}/reviews${SCOPE}`);
            expect(JSON.parse(hm.requests[0].body).previous_review_id).toBe('r0');
        }
    });
});

describe('queue, reconciliation and guards', () => {
    it('sends one request at a time, then reads reviews and block-links again; buttons stay locked', async () => {
        const env = await ready({hold: true});
        select(env, ['o1'], ['n1', 'n2']);
        const pending = env.store.connect('spoke');
        await flush();
        expect(env.posts()).toHaveLength(1);
        expect(env.S.edit.busy).toBe(true);
        expect(env.S.edit.status.text).toBe('записываю… 0 из 2');
        expect((await env.store.regionDecision('HUMAN_CONFIRMED')).ok).toBe(false);   // locked while writing
        env.waiting.shift()();
        await flush(); await flush();
        expect(env.posts()).toHaveLength(2);
        expect(env.S.edit.status.text).toBe('записываю… 1 из 2');
        const before = env.calls.length;
        env.waiting.shift()();
        await pending;
        expect(env.calls.slice(before).map(c => c.url)).toEqual([`${HM}/reviews${SCOPE}`, `${HM}/block-links${SCOPE}`]);
        expect(env.S.edits.map(e => e.new_block_id)).toEqual(['n1', 'n2']);   // shown from the server, not optimistic
        expect(env.S.edit.busy).toBe(false);
        expect(env.S.edit.status.text).toMatch(/^✓ Записано в историю прогона a631b49a · \d\d:\d\d$/);
        expect(env.S.edit.status.notes).toEqual(['Связь записана, но ещё не закреплена: у региона нет решения']);
        expect(env.S.selection.OLD).toEqual([]);
    });

    it('refuses empty sides, blocks outside the region, pairs without a common region, more than 50 edges', async () => {
        const env = await ready();
        select(env, ['o1', 'o2'], ['n1']);
        expect((await env.store.connect('one')).error).toBe('Для 1→1 выберите ровно один блок OLD и один блок NEW.');
        select(env, ['o1', 'o2'], ['n1', 'n2']);
        expect((await env.store.connect('spoke')).error).toBe('Для 1→N / N→1 одна сторона должна содержать ровно 1 блок.');
        select(env, ['x-outside'], ['n1']);
        expect((await env.store.connect('one')).error).toBe('В выборке есть блок, который не входит ни в один регион: закрепить нельзя');
        select(env, ['o3'], ['n2']);
        expect((await env.store.connect('one')).error)
            .toBe('Эти блоки не входят в один смысловой регион прогона. Связь между регионами в текущей версии сохранить нельзя.');
        select(env, ['o2', 'o3'], ['n1']);
        expect((await env.store.connect('spoke')).error)
            .toBe('Для части пар нет общего региона (1 из 2). Создайте их по одной или выберите блоки одного региона.');
        select(env, Array.from({length: 8}, (_, i) => 'o' + i), Array.from({length: 7}, (_, i) => 'n' + i));
        expect((await env.store.connect('cartesian')).error).toBe('За одно действие можно создать не более 50 связей. Выбрано 56 — уменьшите выбор.');
        select(env, ['o3'], []);
        env.store.focusRegion('R');
        expect((await env.store.regionDecision('HUMAN_CONFIRMED')).error)
            .toBe('В выборке есть блоки вне региона R (1) — снимите их или выберите другой регион.');
        expect(env.posts()).toEqual([]);
    });

    it('asks which region when both ends belong to several regions', async () => {
        const env = await ready();
        select(env, ['o1'], ['n1']);
        env.store.toggleEdit(true);
        expect(env.store.connectHint()).toBe('Эти блоки входят в несколько регионов: R, Q. Одно и то же ребро в двух регионах — '
            + 'две независимые связи; разные решения по ним дадут конфликт.');
        expect((await env.store.connect('one')).pending).toBe('region');
        await env.store.chooseRegion('Q');
        expect(JSON.parse(env.posts()[0].body).region_id).toBe('Q');
        select(env, ['o2'], ['n2']);
        expect(env.store.connectHint()).toBe('Связь будет создана в регионе R');
    });
});

describe('answers of the HM API (C10, UX_STATE_MATRIX §4.2)', () => {
    async function answered(answer, action = env => env.store.connect('one'), options = {}) {
        const env = await ready({answer, ...options});
        select(env, ['o1'], ['n2']);
        env.store.setComment('мой комментарий');
        const result = await action(env);
        return {env, result};
    }

    it('reads the code from {detail:{error}}, {detail:"text"} and legacy {error}', async () => {
        const cases = [
            [{code: 400, body: {detail: {error: 'BLOCK_LINK_ALREADY_EXISTS', ok: false}}}, 'Такая связь уже есть.'],
            [{code: 400, body: {detail: {error: 'OLD_BLOCK_NOT_IN_REGION', ok: false}}}, 'Блок не входит в регион R.'],
            [{code: 400, body: {detail: {error: 'REGION_NOT_FOUND', ok: false}}}, 'Регион R не найден в результате a631b49a — обновите страницу.'],
            [{code: 400, body: {detail: {error: 'INVALID_SCOPE_ID', kind: 'object', ok: false}}},
                'Недопустимый идентификатор объекта или пары документов. Ничего не записано — обновите страницу.'],
            [{code: 400, body: {detail: {error: 'BAD_BLOCK_LINK_EVENT', ok: false}}}, 'Сервер отклонил запрос как некорректный. Ничего не записано.'],
            [{code: 400, body: {detail: 'Object and comparison required'}}, 'Запрос не указал объект и пару документов. Ничего не записано — обновите страницу.'],
            [{code: 404, body: {detail: 'Human Mapping result unavailable'}}, 'Результат анализа недоступен — обновите страницу.'],
            [{code: 404, body: {detail: 'HUMAN_MAPPING_UI_DATA_NOT_FOUND'}}, 'Данные смысловых блоков прогона недоступны.'],
            [{code: 400, body: {error: 'BLOCK_LINK_NOT_FOUND'}}, 'Связь уже удалена или изменена — показано актуальное состояние.'],
        ];
        for (const [reply, text] of cases) {
            const {env, result} = await answered(() => reply);
            expect(result.ok, text).toBe(false);
            expect(env.S.edit.status.text).toBe(text);
            expect(env.S.selection).toMatchObject({OLD: ['o1'], NEW: ['n2']});   // kept after a failure
            expect(env.S.edit.comment).toBe('мой комментарий');
        }
    });

    it('translates the review guard of the server (B5) and "Bad review"', async () => {
        const decide = async env => { env.store.focusRegion('R'); await env.store.regionDecision('HUMAN_CONFIRMED'); return env.store.commitDialog(); };
        let {env} = await answered(() => ({code: 400, body: {detail: {error: 'REVIEW_BLOCK_NOT_IN_REGION', ok: false}}}), decide);
        expect(env.S.edit.status.text).toBe('Блок не входит в регион R.');
        ({env} = await answered(() => ({code: 400, body: {detail: 'Bad review'}}), decide));
        expect(env.S.edit.status.text).toBe('Сервер отклонил запрос как некорректный. Ничего не записано.');
    });

    it('HTTP 200 {"error":"NO_CHANGE","ok":true} is not an error; a reassign to the same block sends nothing', async () => {
        const {env, result} = await answered(() => ({code: 200, body: {error: 'NO_CHANGE', ok: true}}));
        expect(result.ok).toBe(true);
        expect(env.S.edit.status).toMatchObject({kind: 'done', text: 'Связь уже указывает на этот блок — ничего не изменено.'});
        const edits = [addEvent('e0', 'human:x', 'o1', 'n1', 1)];
        const same = await ready({edits});
        same.store.selectLink('o1|n1');
        same.store.startReassign('NEW');
        expect((await same.store.reassignTo('NEW', 'n1')).noop).toBe(true);
        expect(same.posts()).toEqual([]);
        expect(same.S.edit.status.text).toBe('Связь уже указывает на этот блок — ничего не изменено.');
    });

    it('5xx: «Сервер не ответил…» and a retry after the reconciling read', async () => {
        let fail = true;
        const {env} = await answered(() => (fail ? {code: 503, body: {detail: 'busy'}} : null));
        expect(env.S.edit.status.text).toBe('Сервер не ответил. Выбор и комментарий сохранены.');
        expect(env.S.edit.status.retryLabel).toBe('Повторить');
        fail = false;
        await env.store.retryWrite();
        expect(env.posts()).toHaveLength(2);
        const [first, second] = env.posts().map(p => JSON.parse(p.body));
        expect({...second, link_id: first.link_id}).toEqual(first);   // the same edge, a fresh human:<uuid> like a new click
        expect(second.link_id).toMatch(/^human:[0-9a-f-]{36}$/);
        expect(env.S.edits).toHaveLength(1);
    });

    it('unknown outcome: checks whether the request was stored before offering a retry', async () => {
        // The request reached the server, the answer did not.
        const stored = await ready({answer: (call, n) => (n === 1 ? 'reject' : null)});
        stored.server.edits.push(addEvent('ev', 'human:aaaaaaaa-0000-4000-8000-000000000001', 'o1', 'n2', 9));
        select(stored, ['o1'], ['n2']);
        const res = await stored.store.connect('one');
        expect(res).toMatchObject({ok: true, confirmed: true});
        expect(stored.S.edit.status.text).toBe('Запись подтверждена');
        // Nothing reached the server.
        const lost = await ready({answer: (call, n) => (n === 1 ? 'reject' : null)});
        select(lost, ['o1'], ['n2']);
        await lost.store.connect('one');
        expect(lost.S.edit.status).toMatchObject({text: 'Не записано —', retryLabel: 'Повторить'});
        await lost.store.retryWrite();
        expect(lost.server.edits).toHaveLength(1);
    });

    it('partial fan: «Сохранено 1 из 2…», the retry sends only what is missing', async () => {
        const env = await ready({answer: (call, n) => (n === 2 ? {code: 500, body: {detail: 'x'}} : null)});
        select(env, ['o1'], ['n1', 'n2']);
        await env.store.connect('spoke');
        expect(env.S.edit.status).toMatchObject({text: 'Сохранено 1 из 2. Остальные не сохранены:', retryLabel: 'Повторить оставшиеся',
            cause: 'Сервер не ответил. Выбор и комментарий сохранены.'});
        expect(env.S.selection).toMatchObject({OLD: ['o1'], NEW: ['n1', 'n2']});
        await env.store.retryWrite();
        expect(env.posts().map(p => JSON.parse(p.body).new_block_id)).toEqual(['n1', 'n2', 'n2']);
        expect(env.server.edits.map(e => e.new_block_id)).toEqual(['n1', 'n2']);
        expect(env.S.edit.status.kind).toBe('done');
    });

    it('another window wrote meanwhile: the history on screen is the server one, with the notice', async () => {
        const env = await ready({answer: () => null});
        env.server.reviews.push(reviewRow('foreign', 'HUMAN_UNCERTAIN', ['o1'], ['n1'], 30));
        select(env, ['o1'], ['n2']);
        await env.store.connect('one');
        expect(env.S.edit.status.notes).toContain('История изменена в другом окне (например, в Human Mapping) — показано актуальное состояние.');
        expect(env.S.reviews.map(r => r.review_id)).toEqual(['foreign']);
    });
});

describe('decisions with preview (planReview) and choices (compileDecision)', () => {
    it('confirming a link shows «было → станет», writes ADD then the review', async () => {
        const edits = [addEvent('e0', 'human:x', 'o1', 'n1', 1), addEvent('e1', 'human:y', 'o2', 'n2', 2)];
        const reviews = [reviewRow('r0', 'HUMAN_CONFIRMED', ['o1'], ['n1'], 3)];
        const env = await ready({edits, reviews});
        env.store.focusRegion('R');
        env.store.selectLink('o2|n2');
        expect((await env.store.decideLink('CONFIRM_LINK')).pending).toBe('preview');
        const dialog = env.S.edit.dialog;
        expect(dialog.title).toBe('Подтвердить связи региона R');
        expect(dialog.lines[0].head).toBe('Будут закреплены как якоря (2):');
        expect(dialog.lines[0].items.map(i => i.tag)).toEqual(['уже закреплена', '']);
        expect(dialog.lines.map(l => l.head)).toContain('Дополнительно попадут в решение из-за выбора блоков:');
        expect(dialog.notes[0]).toMatch(/^Заменяется: ✓ Подтверждено \d\d\.\d\d \d\d:\d\d \(1 якорь\)\. Прежнее решение останется в журнале\.$/);
        expect(dialog.notes).toContain('Решение действует на регион R целиком: связей в регионе 2, из них вне этой пары листов 0.');
        expect(dialog.force).toBe(false);
        await env.store.commitDialog();
        const bodies = env.posts().map(p => JSON.parse(p.body));
        expect(bodies).toEqual([expect.objectContaining({status: 'HUMAN_CONFIRMED', old_block_ids: ['o1', 'o2'],
            new_block_ids: ['n1', 'n2'], previous_review_id: 'r0'})]);
        expect(env.S.edit.dialog).toBeNull();
    });

    it('rejecting a link in a region with anchors asks: delete only / reject replacing / cancel', async () => {
        const edits = [addEvent('e0', 'human:x', 'o1', 'n1', 1), addEvent('e1', 'human:y', 'o2', 'n2', 2)];
        const reviews = [reviewRow('r0', 'HUMAN_CONFIRMED', ['o1', 'o2'], ['n1', 'n2'], 3)];
        const env = await ready({edits, reviews});
        env.store.focusRegion('R');
        env.store.selectLink('o2|n2');
        await env.store.decideLink('REJECT_LINK');
        expect(env.S.edit.dialog).toMatchObject({kind: 'reject-choice', title: 'Отклонить связь OLD текст стр. 20 → NEW таблица стр. 10',
            text: '⚠ В регионе R подтверждены 2 связи. У региона в текущей версии одно решение: отклонение снимет подтверждения (история сохранится).'});
        await env.store.chooseLinkOption('DELETE_ONLY');
        expect(JSON.parse(env.posts()[0].body)).toMatchObject({event_type: 'DELETE_BLOCK_LINK', link_id: 'human:y'});
        const again = await ready({edits, reviews});
        again.store.focusRegion('R');
        again.store.selectLink('o2|n2');
        await again.store.decideLink('REJECT_LINK');
        await again.store.chooseLinkOption('REJECT_REPLACING');
        expect(again.S.edit.dialog.notes).toContain('Запрещена будет только связь OLD текст стр. 20 → NEW таблица стр. 10. '
            + 'Эти блоки могут быть связаны с другими блоками.');
        expect(again.S.edit.dialog.lines.find(l => l.head === 'Перестанут действовать:').items).toHaveLength(1);
        await again.store.commitDialog();
        expect(JSON.parse(again.posts()[0].body)).toMatchObject({status: 'HUMAN_REJECTED', old_block_ids: ['o2'], new_block_ids: ['n2']});
        const cancel = await ready({edits, reviews});
        cancel.store.focusRegion('R');
        cancel.store.selectLink('o2|n2');
        await cancel.store.decideLink('REJECT_LINK');
        await cancel.store.chooseLinkOption('CANCEL');
        expect(cancel.S.edit.dialog).toBeNull();
        expect(cancel.posts()).toEqual([]);
    });

    it('a region verdict without links warns (state E); D-14 conflict asks «Записать всё равно»', async () => {
        const env = await ready();
        env.store.focusRegion('R');
        await env.store.regionDecision('HUMAN_CONFIRMED');
        expect(env.S.edit.dialog.zero).toBe('⚠ В регионе нет связей блоков. Подтверждение ничего не закрепит для нового анализа. '
            + 'Сначала соедините блоки пунктиром.');
        expect(env.S.edit.dialog.force).toBe(true);
        // o1→n1 is forbidden in Q; confirming it in R would break the bridge snapshot.
        const edits = [addEvent('e0', 'human:x', 'o1', 'n1', 1), addEvent('e1', 'human:q', 'o1', 'n1', 2, 'Q')];
        const reviews = [reviewRow('rq', 'HUMAN_REJECTED', ['o1'], ['n1'], 3, 'Q')];
        const conflict = await ready({edits, reviews});
        conflict.store.focusRegion('R');
        conflict.store.selectLink('o1|n1');
        await conflict.store.decideLink('CONFIRM_LINK');
        expect(conflict.S.edit.dialog.conflicts).toEqual(['Связь OLD текст стр. 20 → NEW таблица стр. 10 уже запрещена в регионе Q. '
            + 'Если закрепить её в R, снимок для нового анализа не соберётся, пока одно из решений не будет заменено.']);
        expect(conflict.S.edit.dialog.force).toBe(true);
        await conflict.store.commitDialog();
        expect(conflict.posts()).toHaveLength(1);
    });

    it('«Подтвердить вместе с остальными (n)» re-covers links newer than the verdict', async () => {
        const edits = [addEvent('e0', 'human:x', 'o1', 'n1', 1), addEvent('e1', 'human:y', 'o2', 'n2', 9)];
        const reviews = [reviewRow('r0', 'HUMAN_CONFIRMED', ['o1', 'o2'], ['n1', 'n2'], 5)];
        const env = await ready({edits, reviews});
        expect(env.store.reconfirmCount('R')).toBe(2);
        await env.store.reconfirm('R');
        await env.store.commitDialog();
        expect(JSON.parse(env.posts()[0].body)).toMatchObject({status: 'HUMAN_CONFIRMED', old_block_ids: ['o1', 'o2'], previous_review_id: 'r0'});
    });
});

describe('no write controls without the right to write', () => {
    // The workspace template rendered to VNodes (no DOM) for a given store state.
    function writeButtons(env, width = 1440) {
        const ctx = {console, innerWidth: width, setTimeout, clearTimeout, URLSearchParams, Uint8Array};
        ctx.globalThis = ctx;
        ctx.window = ctx;
        ctx.document = {createElement: () => {
            let h = '';
            return {set innerHTML(v) { h = v; }, get innerHTML() { return h; }, get textContent() { return h; },
                get children() { const m = /^<div foo="([\s\S]*)">$/.exec(h); return [{getAttribute: () => (m ? m[1].replace(/&quot;/g, '"').replace(/&amp;/g, '&') : '')}]; }};
        }};
        vm.createContext(ctx);
        vm.runInContext(vueSource, ctx);
        vm.runInContext(coreSource, ctx);
        vm.runInContext(source, ctx);
        let definition;
        ctx.StageBlockMapping.register({component: (name, def) => { if (name === 'semantic-block-workspace') definition = def; }});
        const props = ctx.Vue.reactive({store: env.store, rows: [ROW], currentRow: ROW, mapBuilt: true, sheetEntry: null});
        const state = definition.setup(props, {emit: () => {}});
        const proxy = ctx.Vue.proxyRefs({...state, ...ctx.Vue.toRefs(props)});
        const found = [];
        (function walk(node) {
            if (!node) return;
            if (Array.isArray(node)) { node.forEach(walk); return; }
            if (node.props && node.props['data-sbm-write']) found.push(node.props['data-sbm-write']);
            if (node.children && typeof node.children === 'object') walk(node.children);
        })(ctx.Vue.compile(definition.template).call(proxy, proxy, []));
        return found;
    }

    it('shows the write controls only for a writable live run with the flag on', async () => {
        const env = await ready({edits: [addEvent('e0', 'human:x', 'o1', 'n1', 1)]});
        env.store.focusRegion('R');
        env.store.selectLink('o1|n1');
        expect(env.store.writeAccess()).toBe(true);
        const full = writeButtons(env);
        for (const name of ['edit-toggle', 'connect-one', 'connect-spoke', 'connect-cartesian', 'link-confirm', 'link-reject',
            'link-uncertain', 'link-delete', 'link-reassign-old', 'link-reassign-new', 'comment', 'region-confirm', 'region-reject',
            'region-uncertain']) expect(full, name).toContain(name);
        // 600–899 px: decisions only, no creation or reassignment; below 600 px: read only.
        const single = writeButtons(env, 700);
        expect(single).toContain('region-confirm');
        expect(single).toContain('link-confirm');
        expect(single).not.toContain('connect-one');
        expect(single).not.toContain('link-reassign-old');
        expect(writeButtons(env, 480)).toEqual([]);
    });

    it('flag off (state L), a run the server does not allow, or a snapshot: no write controls and no POST', async () => {
        const edits = [addEvent('e0', 'human:x', 'o1', 'n1', 1)];
        for (const options of [{status: statusBody({writes: false})},
            {status: statusBody({run: {writable: false, write_block_reason: 'SOURCE_PDF_CHANGED', write_block_reasons: ['SOURCE_PDF_CHANGED'],
                pdf_match: false}})}]) {
            const env = await ready({...options, edits});
            env.store.focusRegion('R');
            env.store.selectLink('o1|n1');
            select(env, ['o2'], ['n2']);
            expect(env.store.writeAccess()).toBe(false);
            expect(writeButtons(env)).toEqual([]);
            for (const act of [() => env.store.connect('one'), () => env.store.regionDecision('HUMAN_CONFIRMED'),
                () => env.store.decideLink('CONFIRM_LINK'), () => env.store.deleteLink()]) expect((await act()).ok).toBe(false);
            expect(env.posts()).toEqual([]);
        }
        const snap = portal({status: statusBody({snapshots: [{result_id: SNAP, hm_available: true, hm_url: '/human-mapping/?x',
            writable: false, write_block_reason: 'SNAPSHOT_READ_ONLY'}]}), edits});
        await snap.store.load({sessionId: SID, pairId: PID});
        await snap.store.bind({kind: 'SNAP', resultId: SNAP});
        expect(snap.store.writeAccess()).toBe(false);
        expect(writeButtons(snap)).toEqual([]);
        expect((await snap.store.regionDecision('HUMAN_CONFIRMED')).ok).toBe(false);
        expect(snap.posts()).toEqual([]);
    });
});
