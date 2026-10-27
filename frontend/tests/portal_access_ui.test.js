/**
 * Портал для viewer (план доступа, 6.3): интерфейс не вызывает запрещённого.
 *
 * Код app.js живёт внутри setup() Vue, поэтому, как и в соседних тестах,
 * нужные функции вырезаются из исходника и исполняются в vm с заглушками.
 * Сетевых запросов и моделей нет.
 */
import {afterEach, describe, expect, it, vi} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');

function between(src, start, end) {
    const from = src.indexOf(start);
    if (from < 0) throw new Error('нет начала: ' + start);
    const to = src.indexOf(end, from + start.length);
    if (to < 0) throw new Error('нет конца: ' + end);
    return src.slice(from, to);
}

function extractFunction(src, name) {
    const start = src.search(new RegExp(`(async )?function ${name}\\(`));
    if (start < 0) throw new Error('нет функции ' + name);
    const brace = src.indexOf('{', src.indexOf(')', start));
    let depth = 0;
    for (let i = brace; i < src.length; i += 1) {
        if (src[i] === '{') depth += 1;
        if (src[i] === '}') { depth -= 1; if (depth === 0) return src.slice(start, i + 1); }
    }
    throw new Error('несбалансирована ' + name);
}

const helpers = between(app, '// ─── Доступ к порталу (план доступа, 6.3)', 'const app = createApp(');
const viewerCan = (sections = {}) => ({
    operate: () => false, admin: () => false,
    view: s => !!(sections[s] && sections[s].view),
    download: s => !!(sections[s] && sections[s].view && sections[s].download),
});
const adminCan = () => ({operate: () => true, admin: () => true, view: () => true, download: () => true});

function helpersContext(extra = {}) {
    const ctx = vm.createContext({...extra});
    vm.runInContext(helpers + '\nglobalThis.H = {portalRouteSection, portalFirstAllowedRoute, '
        + 'portalUrlHasExplicitObject, portalWithObject, portalWsCloseAction, portalViewerBlocksRequest, '
        + 'portalCanOperate, portalCanAdmin, portalCanView, portalCanDownload, portalCanImportDecisions};', ctx);
    return ctx;
}

describe('чистые помощники доступа', () => {
    const {H} = helpersContext();

    it('сотруднику разрешён только POST импорта решений, остальные записи закрыты', () => {
        expect(H.portalViewerBlocksRequest('POST', '/api/knowledge-base/upload-excel?version_id=v2', true)).toBe(false);
        expect(H.portalViewerBlocksRequest('POST', '/api/knowledge-base/upload-excel', false)).toBe(true);
        expect(H.portalViewerBlocksRequest('DELETE', '/api/knowledge-base/upload-excel', true)).toBe(true);
        expect(H.portalViewerBlocksRequest('POST', '/api/audit/batch', true)).toBe(true);
        expect(H.portalViewerBlocksRequest('POST', '/api/knowledge-base/expert-review/P', true)).toBe(true);
    });

    it('право календаря открывает только очередь и планы', () => {
        const allowed = (method, path) => H.portalViewerBlocksRequest(method, path, false, false, true);
        expect(allowed('PUT', '/api/schedule/plan')).toBe(false);
        expect(allowed('POST', '/api/schedule/reserve/operations?object_id=x')).toBe(false);
        expect(allowed('DELETE', '/api/schedule/plan')).toBe(true);
        expect(allowed('POST', '/api/schedule/plan')).toBe(true);
        expect(allowed('POST', '/api/audit/batch')).toBe(true);
        expect(allowed('PUT', '/api/admin/accounts/4/edit-schedule')).toBe(true);
        expect(H.portalViewerBlocksRequest('PUT', '/api/schedule/plan')).toBe(true);
    });

    it('относит маршруты к разделам, служебные страницы — к admin', () => {
        expect(H.portalRouteSection('/')).toBe('audit_results');
        expect(H.portalRouteSection('/project/АР-1/findings')).toBe('audit_results');
        expect(H.portalRouteSection('/section/АР/optimization?tab=history')).toBe('audit_results');
        expect(H.portalRouteSection('/version-comparison')).toBe('version_comparison');
        expect(H.portalRouteSection('/stage-comparison')).toBe('stage_comparison');
        expect(H.portalRouteSection('/knowledge-base')).toBe('knowledge_base');
        expect(H.portalRouteSection('/schedule')).toBe('schedule');
        for (const admin of ['/queue', '/distributed', '/distributed/workers', '/critic-v2-ui',
            '/project/АР-1/log', '/project/АР-1/critic-v2']) {
            expect(H.portalRouteSection(admin)).toBeNull();
        }
    });

    it('выбирает первый разрешённый раздел или null', () => {
        expect(H.portalFirstAllowedRoute(s => s === 'schedule' || s === 'stage_comparison')).toBe('/stage-comparison');
        expect(H.portalFirstAllowedRoute(() => true)).toBe('/');
        expect(H.portalFirstAllowedRoute(() => false)).toBeNull();
    });

    it('видит объект в пути или query и не путает его с отсутствием', () => {
        expect(H.portalUrlHasExplicitObject('/api/stage-comparison/objects/o1/session')).toBe(true);
        expect(H.portalUrlHasExplicitObject('/api/version-comparison/objects/o1/catalog')).toBe(true);
        expect(H.portalUrlHasExplicitObject('/api/external-register/o1/registers')).toBe(true);
        expect(H.portalUrlHasExplicitObject('/api/project-groups?object_id=o1')).toBe(true);
        expect(H.portalUrlHasExplicitObject('/api/projects')).toBe(false);
        expect(H.portalUrlHasExplicitObject('/api/version-comparison/objects')).toBe(false);
        expect(H.portalUrlHasExplicitObject('/api/external-register/_/registers')).toBe(false);
        expect(H.portalUrlHasExplicitObject('/api/findings/x?object_id=')).toBe(false);
    });

    it('withObject дописывает object_id только к /api/ без явного объекта', () => {
        expect(H.portalWithObject('/api/document/АР-1/pdf', 'o 1')).toBe('/api/document/АР-1/pdf?object_id=o%201');
        expect(H.portalWithObject('/api/tiles/p/blocks/image/b1?version_id=v2', 'o1'))
            .toBe('/api/tiles/p/blocks/image/b1?version_id=v2&object_id=o1');
        expect(H.portalWithObject('/api/document/p/pdf#page=3', 'o1')).toBe('/api/document/p/pdf?object_id=o1#page=3');
        expect(H.portalWithObject('/api/x?object_id=o2', 'o1')).toBe('/api/x?object_id=o2');
        expect(H.portalWithObject('/api/version-comparison/objects/o2/runs/r', 'o1')).toBe('/api/version-comparison/objects/o2/runs/r');
        expect(H.portalWithObject('https://cdn.example/x.png', 'o1')).toBe('https://cdn.example/x.png');
        expect(H.portalWithObject('/api/projects', '')).toBe('/api/projects');
    });

    it('коды закрытия сокета: 4401 — на вход, 4403/1008 — без переподключения', () => {
        expect(H.portalWsCloseAction(4401)).toBe('login');
        expect(H.portalWsCloseAction(4403)).toBe('stop');
        expect(H.portalWsCloseAction(1008)).toBe('stop');
        expect(H.portalWsCloseAction(1006)).toBe('reconnect');
        expect(H.portalWsCloseAction(undefined)).toBe('reconnect');
    });

    it('viewer не отправляет изменяющих запросов к /api/, кроме /api/auth/', () => {
        expect(H.portalViewerBlocksRequest('POST', '/api/stage-comparison/sessions')).toBe(true);
        expect(H.portalViewerBlocksRequest('put', '/api/x')).toBe(true);
        expect(H.portalViewerBlocksRequest('GET', '/api/x')).toBe(false);
        expect(H.portalViewerBlocksRequest('POST', '/api/auth/logout')).toBe(false);
        expect(H.portalViewerBlocksRequest(undefined, '/api/x')).toBe(false);
    });

    it('без portal_auth.js всё разрешено, у viewer — только назначенное', () => {
        expect(H.portalCanOperate()).toBe(true);
        expect(H.portalCanView('schedule')).toBe(true);
        const {H: V} = helpersContext({window: {portalCan: viewerCan({schedule: {view: true, download: false}})}});
        expect(V.portalCanOperate()).toBe(false);
        expect(V.portalCanAdmin()).toBe(false);
        expect(V.portalCanView('schedule')).toBe(true);
        expect(V.portalCanView('audit_results')).toBe(false);
        expect(V.portalCanDownload('schedule')).toBe(false);
    });
});

describe('сохранение плана с отдельным правом календаря', () => {
    function harness(allowed) {
        const requests = [];
        const ctx = helpersContext({window: {portalCan: {...viewerCan(), editSchedule: () => allowed}},
            console, JSON, schedPlanEdit: {value: true}, schedPlanSaving: {value: false},
            schedPlanMsg: {value: null}, schedMode: {value: 'month'},
            schedEngineers: {value: [{id: 'expert', name: 'Эксперт'}]},
            schedDraftFor: () => 12, schedLoadPlans: async () => {},
            _schedPeriodRange: () => ({from: '2026-10-01', to: '2026-10-31'}),
            fetch: async (url, init) => { requests.push({url, ...init}); return {ok: true}; },
        });
        vm.runInContext(extractFunction(app, 'schedSavePlans') + '\nglobalThis.save = schedSavePlans;', ctx);
        return {ctx, requests};
    }

    it('сотрудник с правом календаря сохраняет планы без общих прав запуска', async () => {
        const {ctx, requests} = harness(true);
        await ctx.save();
        expect(requests).toHaveLength(1);
        expect(requests[0].url).toBe('/api/schedule/plan');
        expect(requests[0].method).toBe('PUT');
        expect(JSON.parse(requests[0].body).plans).toEqual([{engineer_id: 'expert', engineer_name: 'Эксперт', plan: 12}]);
        expect(ctx.schedPlanEdit.value).toBe(false);
    });

    it('просмотр без права редактирования не отправляет PUT', async () => {
        const {ctx, requests} = harness(false);
        await ctx.save();
        expect(requests).toHaveLength(0);
    });
});

describe('перехват fetch: X-Object-Id и запрет записи у viewer', () => {
    const wrapper = extractFunction(`function patch() {${between(app, '        if (!window.__objHeaderPatched) {',
        '        // Выбор объекта в шапке.')}}`, 'patch');

    function harness(portalCan) {
        const calls = [];
        const forbidden = [];
        const window = {fetch: (input, init) => { calls.push({input, init}); return Promise.resolve({ok: true}); }};
        if (portalCan) window.portalCan = portalCan;
        const ctx = helpersContext({window, Headers, Response, Promise, JSON,
            currentObjectId: {value: 'obj-1'}, showPortalForbidden: d => forbidden.push(d)});
        vm.runInContext(wrapper + '\npatch();', ctx);
        return {fetch: ctx.window.fetch, calls, forbidden};
    }

    it('ставит X-Object-Id только когда объект не указан в URL', async () => {
        const h = harness();
        await h.fetch('/api/projects');
        await h.fetch('/api/stage-comparison/objects/obj-2/session');
        await h.fetch('/api/project-groups?object_id=obj-1');
        await h.fetch('https://cdn.example/a.png');
        const header = i => (h.calls[i].init && h.calls[i].init.headers
            ? h.calls[i].init.headers.get('X-Object-Id') : null);
        expect(header(0)).toBe('obj-1');
        expect(header(1)).toBeNull();
        expect(header(2)).toBeNull();
        expect(header(3)).toBeNull();
    });

    it('у viewer изменяющий запрос не уходит в сеть и получает 403', async () => {
        const h = harness(viewerCan());
        const resp = await h.fetch('/api/stage-comparison/sessions', {method: 'POST', body: '{}'});
        expect(h.calls).toHaveLength(0);
        expect(resp.status).toBe(403);
        expect(h.forbidden).toHaveLength(1);
        await h.fetch('/api/projects');
        await h.fetch('/api/auth/logout', {method: 'POST'});
        expect(h.calls.map(c => c.input)).toEqual(['/api/projects', '/api/auth/logout']);
    });

    it('сотрудник отправляет Excel, но запуск обработки по-прежнему блокируется', async () => {
        const can = {...viewerCan(), importDecisions: () => true};
        const h = harness(can);
        await h.fetch('/api/knowledge-base/upload-excel?project_id=P&version_id=v2', {method: 'POST', body: 'xlsx'});
        const blocked = await h.fetch('/api/audit/batch', {method: 'POST', body: '{}'});
        expect(h.calls.map(c => c.input)).toEqual(['/api/knowledge-base/upload-excel?project_id=P&version_id=v2']);
        expect(blocked.status).toBe(403);
    });

    it('право загрузки открывает только загрузку файлов и её проверку', async () => {
        const h = harness({...viewerCan(), uploadProjects: () => true});
        await h.fetch('/api/projects/upload-folder/precheck', {method: 'POST'});
        await h.fetch('/api/projects/upload-folder', {method: 'POST'});
        expect(h.calls).toHaveLength(2);
        expect((await h.fetch('/api/audit/batch', {method: 'POST'})).status).toBe(403);
        expect((await h.fetch('/api/projects/register-external', {method: 'POST'})).status).toBe(403);
        expect((await h.fetch('/api/projects/upload-folder', {method: 'DELETE'})).status).toBe(403);
    });

    it('admin пишет как раньше', async () => {
        const h = harness(adminCan());
        await h.fetch('/api/stage-comparison/sessions', {method: 'POST', body: '{}'});
        expect(h.calls).toHaveLength(1);
    });
});

describe('WebSocket: коды закрытия', () => {
    function harness(fnName, arg) {
        const sockets = [];
        const timers = [];
        const location = {protocol: 'https:', host: 'audit.example', replace: vi.fn()};
        const ctx = helpersContext({
            location, window: {location}, console: {log() {}, error() {}},
            setTimeout: (fn, ms) => { timers.push(ms); return timers.length; },
            WebSocket: class { static CONNECTING = 0; static OPEN = 1; constructor(url) { this.url = url; this.readyState = 0; sockets.push(this); } close() {} },
        });
        vm.runInContext(`
            let wsMode = null, wsGlobal = null, wsProject = null, wsCurrentProjectId = null, wsProjectReconnects = 0;
            const wsConnected = {value: true};
            const logProjectId = {value: null};
            const closeProjectWS = () => {}, closeGlobalWS = () => {};
            const fetchPrepareQueue = () => {}, refreshBatchQueue = () => {}, pollLiveStatus = () => {};
            const refreshProjectCardSilently = () => {}, loadProjectLog = () => {}, handleWSMessage = () => {};
            ${extractFunction(app, fnName)}
            globalThis.run = () => ${fnName}(${arg ? JSON.stringify(arg) : ''});`, ctx);
        ctx.run();
        return {socket: sockets[0], timers, location, sockets, run: ctx.run};
    }

    it.each([['connectGlobalWS'], ['connectProjectWS', 'АР-1']])('%s: 4401 — на вход, без переподключения', (fn, arg) => {
        const h = harness(fn, arg);
        h.socket.onclose({code: 4401});
        expect(h.location.replace).toHaveBeenCalledWith('/login?reason=session_revoked');
        expect(h.timers).toHaveLength(0);
    });

    it.each([4403, 1008])('connectGlobalWS: %s — без переподключения', code => {
        const h = harness('connectGlobalWS');
        h.socket.onclose({code});
        expect(h.timers).toHaveLength(0);
        expect(h.location.replace).not.toHaveBeenCalled();
    });

    it.each([4403, 1008])('connectProjectWS: %s — без переподключения', code => {
        const h = harness('connectProjectWS', 'АР-1');
        h.socket.onclose({code});
        expect(h.timers).toHaveLength(0);
    });

    it('обычный обрыв переподключается', () => {
        const h = harness('connectGlobalWS');
        h.socket.onclose({code: 1006});
        expect(h.timers).toEqual([3000]);
    });

    it.each([0, 1])('повторный вызов сохраняет глобальный WS в состоянии %s', readyState => {
        const h = harness('connectGlobalWS');
        h.socket.readyState = readyState;
        h.run();
        expect(h.sockets).toHaveLength(1);
    });

    it('viewer сокет не открывает', () => {
        const ctx = helpersContext({window: {portalCan: viewerCan()}, WebSocket: class { constructor() { throw new Error('нет'); } }});
        vm.runInContext(`let wsMode = null;\n${extractFunction(app, 'connectGlobalWS')}\nglobalThis.run = connectGlobalWS;`, ctx);
        expect(() => ctx.run()).not.toThrow();
    });
});

describe('сравнение стадий у viewer', () => {
    const code = between(app, '        // Read-only двойники для viewer (план доступа 5.3).', '        async function scLoadObjects()');

    function harness(reply, portalCan = viewerCan({stage_comparison: {view: true}})) {
        const requests = [];
        const ref = value => ({value});
        const ctx = helpersContext({
            window: {portalCan}, JSON, encodeURIComponent, Object, String, Promise,
            fetch: async (url, init) => { requests.push({url, method: (init && init.method) || 'GET'}); return reply(url); },
            scSessionRequestToken: 0,
            scSelectedObject: ref({id: 'obj 1', stages: [{name: 'stage_1', pdf_count: 2}, {name: 'stage_2', pdf_count: 3}]}),
            scSession: ref(null), scSessionLoading: ref(false), scSessionError: ref(''),
            scViewerNotPrepared: ref(''), scSessionStale: ref(false),
            scActivePair: ref(null), scPairData: ref(null), scMatchState: ref(null), pcCatalogSourceSessionId: ref(''),
            scPairs: ref([]), scPairRowStates: {}, scDocumentOrder: {left: [], right: []},
            scPendingPairSelection: ref(null), scPairingDirty: ref(false), scPairingMatching: ref(false),
            scPairingSaveError: ref(''), scPairingSaveMessage: ref(''),
            scInvalidatePairOpen() {}, scResetProductionReview() {}, scClearConfirmedDocumentPairs() {},
            scFinishDocumentDrag() {}, scFinishPairRowDrag() {}, scLoadDocumentGroups: async () => false,
            scInitializeDocumentOrder() {}, scApplyDocumentDefaults() {}, scStoredActivePairId: () => '',
            scForgetActivePair() {}, scOpenPair: async () => {},
        });
        vm.runInContext(code.replace('async function scRefreshSession', 'globalThis.scRefreshSession = async function scRefreshSession'), ctx);
        return {ctx, requests};
    }
    const json = (status, body) => ({ok: status < 400, status, json: async () => body});

    it('читает подготовленную сессию GET-запросом по id объекта, без POST и без путей стадий', async () => {
        const h = harness(() => json(200, {id: 's1', session_id: 's1', stale: false, documents: {}}));
        await h.ctx.scRefreshSession();
        expect(h.requests).toEqual([{url: '/api/stage-comparison/objects/obj%201/session', method: 'GET'}]);
        expect(h.ctx.scSession.value.id).toBe('s1');
        expect(h.ctx.scSessionStale.value).toBe(false);
        expect(h.ctx.scViewerNotPrepared.value).toBe('');
    });

    it('not_prepared → сообщение «не подготовлено», сессии нет', async () => {
        const h = harness(() => json(404, {detail: {code: 'not_prepared', message: 'Результат ещё не подготовлен администратором.'}}));
        await h.ctx.scRefreshSession();
        expect(h.ctx.scSession.value).toBeNull();
        expect(h.ctx.scViewerNotPrepared.value).toBe('Сравнение ещё не подготовлено администратором');
        expect(h.ctx.scSessionError.value).toBe('');
        expect(h.requests.every(r => r.method === 'GET')).toBe(true);
    });

    it('stale → признак «есть новые документы»', async () => {
        const h = harness(() => json(200, {id: 's1', stale: true, documents: {}}));
        await h.ctx.scRefreshSession();
        expect(h.ctx.scSessionStale.value).toBe(true);
    });

    it('admin по-прежнему создаёт сессию POST-запросом', async () => {
        const h = harness(() => json(200, {id: 's1', documents: {}}), adminCan());
        h.ctx.scSelectedObject.value.stages = [{name: 'stage_1', pdf_count: 1, path: '/a'}, {name: 'stage_2', pdf_count: 1, path: '/b'}];
        await h.ctx.scRefreshSession();
        expect(h.requests).toEqual([{url: '/api/stage-comparison/sessions', method: 'POST'}]);
    });

    it('доска viewer не автосохраняется, а pcReadOnlySources включает отсутствие права', () => {
        const fn = extractFunction(app, 'scSchedulePairingSave');
        const timers = [];
        const ctx = vm.createContext({window: {portalCan: viewerCan()}, scSession: {value: {id: 's1'}},
            setTimeout: () => timers.push(1), clearTimeout() {}});
        vm.runInContext(`let scPairingSaveTimer = 0;\n${fn}\nglobalThis.run = scSchedulePairingSave;`, ctx);
        ctx.run();
        expect(timers).toHaveLength(0);
        expect(app).toMatch(/const pcReadOnlySources = computed\(\(\) => pcDemo\.value \|\| pcBridgeActive\.value \|\| !canOperate\.value\)|const pcReadOnlySources = computed\(\(\) => pcDemo\.value \|\| pcBridgeActive\.value \|\| !\(typeof window === 'undefined'/);
    });

    it('viewer пару не создаёт — только ищет подготовленную', async () => {
        const fn = extractFunction(app, 'scCreatePairForDocuments');
        const requests = [];
        const ctx = helpersContext({window: {portalCan: viewerCan()}, encodeURIComponent, JSON,
            pcBridgeActive: {value: false}, scSelectedObject: {value: {id: 'o1'}},
            fetch: async (url, init) => { requests.push([url, (init && init.method) || 'GET']);
                return {ok: false, status: 404, json: async () => ({detail: {code: 'not_prepared'}})}; }});
        vm.runInContext(between(app, '        function scViewerSessionUrl(', '        async function scRefreshSession()')
            + fn + '\nglobalThis.run = scCreatePairForDocuments;', ctx);
        await expect(ctx.run('/p/OLD.pdf', '/p/NEW.pdf', 's1')).rejects.toThrow('Результат ещё не подготовлен администратором');
        expect(requests).toEqual([['/api/stage-comparison/objects/o1/sessions/s1/pair?left=%2Fp%2FOLD.pdf&right=%2Fp%2FNEW.pdf', 'GET']]);
    });
});

describe('«Отчёт с ИИ-разбором» у viewer', () => {
    function harness(portalCan, analysis) {
        const requests = [];
        const timers = new Set();
        const context = vm.createContext({console, clearTimeout,
            setTimeout: (cb, ms) => { const t = setTimeout(cb, ms); timers.add(t); return t; },
            portalCan,
            fetch: async (url, init) => {
                requests.push([url, (init && init.method) || 'GET']);
                const body = url.endsWith('/consolidated')
                    ? {schema: 'projectchange-consolidations/1', consolidations: [{source_run_id: 'src1', consolidator_run_id: 'c1',
                        source_kind: 'PRODUCTION_RUN', source_is_current_run: true, model: 'm', reasoning: 'r', completed_at: 'x'}]}
                    : url.endsWith('/it-analysis') ? analysis : {status: 'NONE'};
                return {ok: true, json: async () => body};
            }});
        vm.runInContext(readFileSync(new URL('../static/js/vue.global.prod.js', import.meta.url), 'utf8'), context);
        vm.runInContext(readFileSync(new URL('../static/js/project-change-consolidation.js', import.meta.url), 'utf8'), context);
        let component;
        context.ProjectChangeConsolidation.register({component: (_, c) => { component = c; }});
        const props = context.Vue.reactive({sessionId: 's1', pairId: 'pair-1'});
        const v = component.setup(props, {emit: () => {}});
        return {v, requests, context, cleanup: () => timers.forEach(clearTimeout)};
    }
    const settle = () => new Promise(r => setTimeout(r, 0));
    let current;
    afterEach(() => current && current.cleanup());

    it('результата нет — сообщение, ни одного POST', async () => {
        current = harness(viewerCan({stage_comparison: {view: true, download: true}}), {available: false});
        await settle();
        await current.v.makeReport();
        expect(current.v.uploadMessage.value).toBe('Результат ещё не подготовлен администратором');
        expect(current.requests.filter(([, m]) => m !== 'GET')).toEqual([]);
        expect(current.v.canOperate).toBe(false);
    });

    it('результат готов — скачивание без запуска', async () => {
        current = harness(viewerCan({stage_comparison: {view: true, download: true}}),
            {schema: 'projectchange-it-analysis-status/1', available: true, failed_ids: [], source: 'model',
                filled: 1, rows: 1, uploaded_at: '2026-10-19'});
        await settle(); await settle();
        current.context.location = {href: ''};
        await current.v.makeReport();
        expect(current.context.location.href).toMatch(/report\.xlsx$/);
        expect(current.requests.filter(([, m]) => m !== 'GET')).toEqual([]);
    });

    it('пересчёт, отмена и загрузка разбора у viewer ничего не отправляют', async () => {
        current = harness(viewerCan({stage_comparison: {view: true}}), {available: true, failed_ids: []});
        await settle();
        current.context.confirm = () => true;
        await current.v.recompute();
        await current.v.cancelRun();
        await current.v.uploadAnalysis({target: {files: [{name: 'x.xlsx'}], value: 'x.xlsx'}});
        expect(current.requests.filter(([, m]) => m !== 'GET')).toEqual([]);
        expect(current.v.canDownload).toBe(false);
    });
});

describe('сравнение версий у viewer: matrix-view', () => {
    const source = readFileSync(new URL('../static/js/version-comparison.js', import.meta.url), 'utf8');
    function page(fetch, portalCan) {
        vi.useFakeTimers();
        const sandbox = {fetch, FormData: class {}, setTimeout, clearTimeout, portalCan,
            Vue: {ref: value => ({value}), reactive: value => value, watch: () => {}, onMounted: () => {}, onBeforeUnmount: () => {}}};
        sandbox.globalThis = sandbox;
        vm.runInNewContext(source, sandbox);
        let component;
        sandbox.VersionComparison.register({component: (_n, value) => { component = value; }});
        const p = component.setup({portalObjectId: null});
        p.objectId.value = 'obj';
        return p;
    }
    const reply = data => ({ok: true, status: 200, json: async () => data});
    afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); });

    it('viewer идёт в matrix-view; not_prepared — без опроса', async () => {
        const fetch = vi.fn(async () => reply({state: 'not_prepared', rows: []}));
        const p = page(fetch, viewerCan({version_comparison: {view: true}}));
        p.projects.value = [{project_id: 'pr', versions: ['v1', 'v2']}];
        p.selectedProject.value = 'pr';
        p.selectProject();
        await vi.advanceTimersByTimeAsync(10000);
        expect(fetch.mock.calls.map(c => c[0])).toEqual(['/api/version-comparison/objects/obj/projects/pr/matrix-view']);
        expect(p.matrices.pr.state).toBe('not_prepared');
        expect(p.canOperate).toBe(false);
    });

    it('viewer: building — опрос продолжается', async () => {
        const fetch = vi.fn(async () => reply({state: 'building', rows: []}));
        const p = page(fetch, viewerCan({version_comparison: {view: true}}));
        p.projects.value = [{project_id: 'pr', versions: ['v1', 'v2']}];
        p.selectedProject.value = 'pr';
        p.selectProject();
        await vi.advanceTimersByTimeAsync(3100);
        expect(fetch.mock.calls.length).toBeGreaterThan(1);
        expect(fetch.mock.calls.every(c => c[0].endsWith('/matrix-view'))).toBe(true);
    });

    it('admin по-прежнему зовёт /matrix', async () => {
        const fetch = vi.fn(async url => reply(url.endsWith('/matrix') ? {state: 'ready', rows: []} : {runs: [], series: []}));
        const p = page(fetch, adminCan());
        p.projects.value = [{project_id: 'pr', versions: ['v1', 'v2']}];
        p.selectedProject.value = 'pr';
        p.selectProject();
        await vi.advanceTimersByTimeAsync(0);
        expect(fetch.mock.calls[0][0]).toBe('/api/version-comparison/objects/obj/projects/pr/matrix');
    });
});

describe('разметка: служебное скрыто, скачивания — по праву', () => {
    it.each([true, false])('загрузка решений сотрудником: право %s', async allowed => {
        const post = vi.fn(async () => ({ok: true, data: {projects: {P: {saved: 1}}}}));
        const ctx = helpersContext({
            window: {portalCan: {...viewerCan(), importDecisions: () => allowed}},
            _postExcelDecisions: post, kbUploadLoading: {value: false},
            currentProjectId: {value: null}, alert: vi.fn(),
        });
        vm.runInContext(extractFunction(app, 'uploadAndApplyDecisions') + '\nglobalThis.upload = uploadAndApplyDecisions;', ctx);
        const file = {name: 'decisions.xlsx'};
        const event = {target: {files: [file], value: 'selected'}};
        await ctx.upload(event);
        expect(post).toHaveBeenCalledTimes(allowed ? 1 : 0);
        if (allowed) {
            expect(post).toHaveBeenCalledWith(file);
            expect(event.target.value).toBe('');
        }
    });

    it('навигация и шапка опираются на права', () => {
        expect(html).toContain(`<div v-if="canOperate" class="nav-item" :class="{active: currentView === 'queue'}"`);
        expect(html).toContain(`v-if="canView('stage_comparison')"`);
        expect(html).not.toContain('class="paid-cost-btn"');
        expect(html).not.toContain('class="account-btn"');
        expect(html).toContain('Доступ к разделам ещё не назначен. Обратитесь к администратору.');
        expect(html).toContain(`<button v-if="canDownload('audit_results')" class="btn-expert-toggle"`);
        expect(html).toContain('<script>window.__portalCaps = {{portal_caps}};</script>');
        expect(html.match(/<label v-if="canImportDecisions"[^>]+title="Загрузить решения из Excel"/g)).toHaveLength(2);
    });
});
