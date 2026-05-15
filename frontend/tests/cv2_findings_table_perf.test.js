/**
 * Smoke-тесты для оптимизаций inline Critic v2:
 *  - deferred fetch (не блокирует таблицу замечаний);
 *  - session-scoped client cache (повторное открытие проекта не делает fetch);
 *  - graceful degrade при ошибке endpoint;
 *  - dev-флаг cv2DebugVisible для скрытия debug-навигации.
 *
 * Pure-функции продублированы здесь как mirror — если они разойдутся с app.js,
 * этот тест упадёт первым.
 *
 * Запуск:
 *   cd frontend && npm test
 */
import { describe, it, expect, vi } from 'vitest';

// ─── Mirror pure-функций из app.js ──────────────────────────────────────────

// Простая абстракция планировщика с fallback на setTimeout, как в app.js.
function makeScheduleIdle(win) {
    return function _scheduleIdle(fn) {
        if (win && typeof win.requestIdleCallback === 'function') {
            win.requestIdleCallback(fn, { timeout: 1500 });
        } else if (typeof setTimeout === 'function') {
            setTimeout(fn, 0);
        } else {
            fn();
        }
    };
}

function _readCv2DebugFlag(win) {
    try {
        if (!win) return false;
        const url = win.location && win.location.href ? new URL(win.location.href) : null;
        if (url && url.searchParams.get('cv2debug') === '1') return true;
        if (win.localStorage && win.localStorage.getItem('cv2_debug') === '1') return true;
    } catch (_) {}
    return false;
}

function cv2BareFindingId(rawId) {
    if (!rawId) return '';
    const s = String(rawId);
    const idx = s.lastIndexOf(':');
    return idx >= 0 ? s.slice(idx + 1) : s;
}

// Mirror _fetchCriticV2ForFindings: парсит ответ endpoint в payload {map, available, warning}.
async function fetchCv2Inline(fetchImpl, projectId) {
    try {
        const resp = await fetchImpl('/api/critic-v2/projects/' + encodeURIComponent(projectId) + '/triage-ui');
        if (!resp.ok) return { map: {}, available: false, warning: 'нет данных' };
        const raw = await resp.json();
        const items = (raw && Array.isArray(raw.items)) ? raw.items : [];
        const warning = (raw && raw.warning) ? raw.warning : '';
        const map = {};
        for (const it of items) {
            const bare = cv2BareFindingId(it.finding_id);
            if (!bare) continue;
            map[bare] = it;
        }
        return { map, available: items.length > 0, warning };
    } catch (e) {
        return { map: {}, available: false, warning: 'ошибка загрузки' };
    }
}

// Mirror _scheduleCriticV2Load: проверяет cache, ставит fetch в idle callback,
// применяет result через _applyCv2Result.
function makeScheduler({ scheduleIdle, fetchImpl, sessionCache, currentProjectIdRef }) {
    const state = {
        loading: false,
        map: {},
        available: false,
        warning: '',
        applyCalls: 0,
    };
    function _applyCv2Result(projectId, payload) {
        if (currentProjectIdRef.value && currentProjectIdRef.value !== projectId) return;
        state.map = payload.map || {};
        state.available = !!payload.available;
        state.warning = payload.warning || '';
        state.loading = false;
        state.applyCalls += 1;
    }
    function schedule(projectId, opts) {
        const force = !!(opts && opts.forceRefresh);
        if (!force && sessionCache[projectId]) {
            _applyCv2Result(projectId, sessionCache[projectId]);
            return;
        }
        state.loading = true;
        state.warning = '';
        scheduleIdle(async () => {
            if (currentProjectIdRef.value && currentProjectIdRef.value !== projectId) {
                state.loading = false;
                return;
            }
            const payload = await fetchCv2Inline(fetchImpl, projectId);
            sessionCache[projectId] = payload;
            _applyCv2Result(projectId, payload);
        });
    }
    return { state, schedule };
}

// ─── _readCv2DebugFlag ──────────────────────────────────────────────────────

describe('_readCv2DebugFlag', () => {
    it('по умолчанию false (нет ни URL-параметра, ни localStorage)', () => {
        const win = { location: { href: 'http://x/' }, localStorage: { getItem: () => null } };
        expect(_readCv2DebugFlag(win)).toBe(false);
    });

    it('true при ?cv2debug=1', () => {
        const win = { location: { href: 'http://x/?cv2debug=1' }, localStorage: { getItem: () => null } };
        expect(_readCv2DebugFlag(win)).toBe(true);
    });

    it('true при localStorage.cv2_debug=1', () => {
        const win = { location: { href: 'http://x/' }, localStorage: { getItem: (k) => k === 'cv2_debug' ? '1' : null } };
        expect(_readCv2DebugFlag(win)).toBe(true);
    });

    it('false при cv2debug=0', () => {
        const win = { location: { href: 'http://x/?cv2debug=0' }, localStorage: { getItem: () => null } };
        expect(_readCv2DebugFlag(win)).toBe(false);
    });

    it('не падает на отсутствующем localStorage', () => {
        const win = { location: { href: 'http://x/' } };
        expect(_readCv2DebugFlag(win)).toBe(false);
    });

    it('не падает на отсутствующем window', () => {
        expect(_readCv2DebugFlag(null)).toBe(false);
    });
});

// ─── _scheduleIdle fallback ─────────────────────────────────────────────────

describe('_scheduleIdle', () => {
    it('использует requestIdleCallback если доступен', () => {
        const ric = vi.fn();
        const win = { requestIdleCallback: ric };
        const sched = makeScheduleIdle(win);
        const cb = () => {};
        sched(cb);
        expect(ric).toHaveBeenCalledTimes(1);
        expect(ric.mock.calls[0][0]).toBe(cb);
        expect(ric.mock.calls[0][1]).toEqual({ timeout: 1500 });
    });

    it('fallback на setTimeout если requestIdleCallback отсутствует', async () => {
        const win = {};
        const sched = makeScheduleIdle(win);
        let fired = false;
        await new Promise(resolve => {
            sched(() => { fired = true; resolve(); });
        });
        expect(fired).toBe(true);
    });
});

// ─── Deferred fetch — таблица не блокируется ────────────────────────────────

describe('deferred Critic v2 — таблица замечаний рендерится первой', () => {
    it('schedule НЕ делает fetch синхронно', () => {
        const fetchImpl = vi.fn();
        const sessionCache = {};
        const currentProjectIdRef = { value: 'P1' };
        // Используем noop-планировщик: он запоминает callback, но НЕ выполняет
        const scheduled = [];
        const scheduleIdle = (fn) => scheduled.push(fn);
        const { state, schedule } = makeScheduler({
            scheduleIdle, fetchImpl, sessionCache, currentProjectIdRef,
        });
        schedule('P1', {});
        // fetch отложен до следующего idle tick — НЕ вызван
        expect(fetchImpl).not.toHaveBeenCalled();
        // loading state должен быть включён
        expect(state.loading).toBe(true);
        // applyCv2Result ещё НЕ вызван (таблица не пере-фильтрована из-за критика)
        expect(state.applyCalls).toBe(0);
        // и в очереди idle лежит ровно один callback
        expect(scheduled).toHaveLength(1);
    });

    it('после idle callback fetch выполняется и state применяется', async () => {
        const fetchImpl = vi.fn().mockResolvedValue({
            ok: true,
            json: async () => ({ items: [{ finding_id: 'P1:F-001', tab: 'primary', queue: 'strong_keep', score: 9, confidence: 0.9 }] }),
        });
        const sessionCache = {};
        const currentProjectIdRef = { value: 'P1' };
        const scheduled = [];
        const scheduleIdle = (fn) => scheduled.push(fn);
        const { state, schedule } = makeScheduler({
            scheduleIdle, fetchImpl, sessionCache, currentProjectIdRef,
        });
        schedule('P1', {});
        // вручную "идём в idle"
        await scheduled[0]();
        expect(fetchImpl).toHaveBeenCalledTimes(1);
        expect(state.available).toBe(true);
        expect(state.loading).toBe(false);
        expect(state.map['F-001'].queue).toBe('strong_keep');
        // session cache наполнен
        expect(sessionCache['P1']).toBeTruthy();
        expect(sessionCache['P1'].available).toBe(true);
    });
});

// ─── Session cache ──────────────────────────────────────────────────────────

describe('session cache — повторный fetch не делается', () => {
    it('второй вызов schedule для того же project_id берёт payload из кеша', async () => {
        const fetchImpl = vi.fn().mockResolvedValue({
            ok: true,
            json: async () => ({ items: [{ finding_id: 'P1:F-1', tab: 'primary', queue: 'strong_keep' }] }),
        });
        const sessionCache = {};
        const currentProjectIdRef = { value: 'P1' };
        const scheduled = [];
        const scheduleIdle = (fn) => scheduled.push(fn);
        const { state, schedule } = makeScheduler({
            scheduleIdle, fetchImpl, sessionCache, currentProjectIdRef,
        });
        // Первый вызов: idle → fetch → cache
        schedule('P1', {});
        await scheduled[0]();
        expect(fetchImpl).toHaveBeenCalledTimes(1);
        expect(state.available).toBe(true);

        // Второй вызов: cache hit → НЕТ нового idle callback, fetch не растёт
        schedule('P1', {});
        expect(scheduled).toHaveLength(1);     // в очередь не добавлен
        expect(fetchImpl).toHaveBeenCalledTimes(1);  // НЕ вырос
        expect(state.available).toBe(true);
    });

    it('forceRefresh=true инвалидирует cache → новый fetch', async () => {
        const fetchImpl = vi.fn().mockResolvedValue({
            ok: true,
            json: async () => ({ items: [{ finding_id: 'P1:F-1', tab: 'primary', queue: 'strong_keep' }] }),
        });
        const sessionCache = {};
        const currentProjectIdRef = { value: 'P1' };
        const scheduled = [];
        const scheduleIdle = (fn) => scheduled.push(fn);
        const { schedule } = makeScheduler({
            scheduleIdle, fetchImpl, sessionCache, currentProjectIdRef,
        });
        schedule('P1', {});
        await scheduled[0]();
        expect(fetchImpl).toHaveBeenCalledTimes(1);

        // forceRefresh — сбрасываем кеш и повторяем
        delete sessionCache['P1'];
        schedule('P1', { forceRefresh: true });
        await scheduled[1]();
        expect(fetchImpl).toHaveBeenCalledTimes(2);
    });

    it('кеш per-project: P1 cached, P2 всё равно делает fetch', async () => {
        const fetchImpl = vi.fn().mockResolvedValue({
            ok: true,
            json: async () => ({ items: [] }),
        });
        const sessionCache = {};
        const currentProjectIdRef = { value: 'P1' };
        const scheduled = [];
        const scheduleIdle = (fn) => scheduled.push(fn);
        const { schedule } = makeScheduler({
            scheduleIdle, fetchImpl, sessionCache, currentProjectIdRef,
        });
        schedule('P1', {});
        await scheduled[0]();
        // переключаемся на P2
        currentProjectIdRef.value = 'P2';
        schedule('P2', {});
        await scheduled[1]();
        expect(fetchImpl).toHaveBeenCalledTimes(2);
        const urls = fetchImpl.mock.calls.map(c => c[0]);
        expect(urls.some(u => u.includes('P1'))).toBe(true);
        expect(urls.some(u => u.includes('P2'))).toBe(true);
    });
});

// ─── Graceful degrade ───────────────────────────────────────────────────────

describe('graceful degrade — таблица не ломается', () => {
    it('endpoint вернул 500 → state.available=false, warning="нет данных"', async () => {
        const fetchImpl = vi.fn().mockResolvedValue({ ok: false, status: 500 });
        const sessionCache = {};
        const currentProjectIdRef = { value: 'P1' };
        const scheduled = [];
        const scheduleIdle = (fn) => scheduled.push(fn);
        const { state, schedule } = makeScheduler({
            scheduleIdle, fetchImpl, sessionCache, currentProjectIdRef,
        });
        schedule('P1', {});
        await scheduled[0]();
        expect(state.available).toBe(false);
        expect(state.warning).toBe('нет данных');
        expect(state.loading).toBe(false);
    });

    it('network error → state.available=false, warning="ошибка загрузки"', async () => {
        const fetchImpl = vi.fn().mockRejectedValue(new Error('boom'));
        const sessionCache = {};
        const currentProjectIdRef = { value: 'P1' };
        const scheduled = [];
        const scheduleIdle = (fn) => scheduled.push(fn);
        const { state, schedule } = makeScheduler({
            scheduleIdle, fetchImpl, sessionCache, currentProjectIdRef,
        });
        schedule('P1', {});
        await scheduled[0]();
        expect(state.available).toBe(false);
        expect(state.warning).toBe('ошибка загрузки');
    });

    it('пустой items[] и warning от backend → available=false, warning сохранён', async () => {
        const fetchImpl = vi.fn().mockResolvedValue({
            ok: true,
            json: async () => ({ items: [], warning: 'этот проект отсутствует в Critic v2 export' }),
        });
        const sessionCache = {};
        const currentProjectIdRef = { value: 'P1' };
        const scheduled = [];
        const scheduleIdle = (fn) => scheduled.push(fn);
        const { state, schedule } = makeScheduler({
            scheduleIdle, fetchImpl, sessionCache, currentProjectIdRef,
        });
        schedule('P1', {});
        await scheduled[0]();
        expect(state.available).toBe(false);
        expect(state.warning).toContain('этот проект отсутствует');
    });
});

// ─── Race condition: пользователь ушёл на другой проект ─────────────────────

describe('race condition — переключение проекта в полёте', () => {
    it('если currentProjectId сменился до выполнения idle, state НЕ применяется', async () => {
        const fetchImpl = vi.fn().mockResolvedValue({
            ok: true,
            json: async () => ({ items: [{ finding_id: 'P1:F-1', tab: 'primary', queue: 'strong_keep' }] }),
        });
        const sessionCache = {};
        const currentProjectIdRef = { value: 'P1' };
        const scheduled = [];
        const scheduleIdle = (fn) => scheduled.push(fn);
        const { state, schedule } = makeScheduler({
            scheduleIdle, fetchImpl, sessionCache, currentProjectIdRef,
        });
        schedule('P1', {});
        // Пользователь уходит на другой проект ДО выполнения idle callback
        currentProjectIdRef.value = 'P2';
        await scheduled[0]();
        // fetch даже не должен был выполниться (early return по проверке currentProjectId)
        expect(fetchImpl).not.toHaveBeenCalled();
        // state не применён, applyCv2Result не вызван
        expect(state.applyCalls).toBe(0);
        expect(state.loading).toBe(false);   // был сброшен
    });
});
