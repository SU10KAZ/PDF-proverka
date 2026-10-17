import {afterEach, describe, expect, it, vi} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../static/js/version-comparison.js', import.meta.url), 'utf8');

function pageWith(fetch) {
    vi.useFakeTimers();
    let unmount;
    const sandbox = {fetch, FormData: class {}, setTimeout, clearTimeout,
        Vue: {ref: value => ({value}), reactive: value => value, watch: () => {},
            onMounted: () => {}, onBeforeUnmount: fn => { unmount = fn; }}};
    sandbox.globalThis = sandbox;
    vm.runInNewContext(source, sandbox);
    let component;
    sandbox.VersionComparison.register({component: (_name, value) => { component = value; }});
    const page = component.setup();
    page.objectId.value = 'obj';
    return {page, unmount: () => unmount()};
}

const reply = (data, status = 200) => ({ok: status < 400, status, json: async () => data});
const project = {project_id: 'project'};
const run = (state, extra = {}) => ({run_id: 'run', state, items: [], ...extra});
const series = (state, extra = {}) => ({series_id: 'series', state, pairs: [], ...extra});
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); });

describe('comparison progress survives recovery and transient failures', () => {
    it('replaces a cancelled summary for the old order with the new adjacent series', async () => {
        const p = {...project, versions: ['v1', 'v2', 'v3']};
        const old = series('cancelled', {series_id: 'old', pairs: [{a: 'v2', b: 'v3'}, {a: 'v3', b: 'v1'}]});
        const latest = series('done', {series_id: 'new', pairs: [{a: 'v1', b: 'v2'}, {a: 'v2', b: 'v3'}]});
        let saved = old;
        const fetch = vi.fn(async url => reply(url.endsWith('/matrix') ? {state: 'ready'} :
            url.endsWith('/runs') ? {runs: []} : {series: [saved]}));
        const {page} = pageWith(fetch);
        page.projects.value = [p]; page.selectedProject.value = p.project_id;
        page.selectProject();
        await vi.advanceTimersByTimeAsync(0);
        expect(page.seriesOf(p).series_id).toBe('old');
        expect(page.seriesMatchesOrder(p, page.seriesOf(p))).toBe(false);
        saved = latest;
        await vi.advanceTimersByTimeAsync(5000);
        expect(page.seriesOf(p).series_id).toBe('new');
        expect(page.seriesMatchesOrder(p, page.seriesOf(p))).toBe(true);
        const count = fetch.mock.calls.length;
        await vi.advanceTimersByTimeAsync(10000);
        expect(fetch).toHaveBeenCalledTimes(count);
    });

    it('does not let an old in-flight poll replace a newly selected series', async () => {
        let finishOld;
        const latest = series('done', {series_id: 'new'});
        const fetch = vi.fn(async url => {
            if (url.endsWith('/resume')) return reply(series('running', {series_id: 'old'}));
            if (url.endsWith('/series/old')) return new Promise(resolve => { finishOld = resolve; });
            if (url.endsWith('/matrix')) return reply({state: 'ready'});
            if (url.endsWith('/runs')) return reply({runs: []});
            return reply({series: [latest]});
        });
        const {page} = pageWith(fetch);
        await page.resumeSeries(project, series('interrupted', {series_id: 'old'}));
        await vi.advanceTimersByTimeAsync(1000);
        page.projects.value = [project]; page.selectedProject.value = project.project_id;
        page.selectProject();
        await vi.advanceTimersByTimeAsync(0);
        expect(page.seriesOf(project).series_id).toBe('new');
        finishOld(reply(series('running', {series_id: 'old'})));
        await vi.advanceTimersByTimeAsync(10000);
        expect(page.seriesOf(project).series_id).toBe('new');
        expect(fetch.mock.calls.filter(([url]) => url.endsWith('/series/old'))).toHaveLength(1);
    });

    it.each([false, true])('updates the open transition when replacing a finished series (manual selection: %s)', async manual => {
        const oldPair = {a: 'v1', b: 'v2', run_id: 'old-run'};
        const newPair = {...oldPair, run_id: 'new-run'};
        let saved = series('done', {series_id: 'old', pairs: [oldPair]});
        const fetch = vi.fn(async url => {
            if (url.endsWith('/matrix')) return reply({state: 'ready'});
            if (url.endsWith('/runs')) return reply({runs: []});
            if (url.endsWith('/reports')) return reply({state: 'none'});
            if (url.endsWith('/runs/new-run')) return reply(run('done', {run_id: 'new-run'}));
            return reply({series: [saved]});
        });
        const {page} = pageWith(fetch);
        page.projects.value = [project]; page.selectedProject.value = project.project_id;
        page.selectProject();
        await vi.advanceTimersByTimeAsync(0);
        page.activeRun.project = run('done', {run_id: manual ? 'manual-run' : 'old-run'});
        page.pairs.project = {a: 'v1', b: 'v2'};
        saved = series('done', {series_id: 'new', pairs: [newPair]});
        page.selectProject();
        await vi.advanceTimersByTimeAsync(0);
        expect(page.seriesOf(project).series_id).toBe('new');
        expect(page.activeRun.project.run_id).toBe(manual ? 'manual-run' : 'new-run');
    });

    it('keeps refreshing an automatically recoverable interrupted run', async () => {
        const fetch = vi.fn()
            .mockResolvedValueOnce(reply(run('interrupted', {auto_resume: true})))
            .mockResolvedValueOnce(reply(run('running')))
            .mockResolvedValueOnce(reply(run('cancelled')));
        const {page} = pageWith(fetch);
        await page.pickRun(project, 'run');
        expect(page.runStateLabel(page.activeRun.project)).toBe('ожидает автоматического продолжения');
        await vi.advanceTimersByTimeAsync(2000);
        expect(page.activeRun.project.state).toBe('running');
        await vi.advanceTimersByTimeAsync(2000);
        expect(page.activeRun.project.state).toBe('cancelled');
        await vi.advanceTimersByTimeAsync(10000);
        expect(fetch).toHaveBeenCalledTimes(3);
    });

    it.each([undefined, 503])('retries a failed run refresh (status %s)', async status => {
        const fetch = vi.fn();
        if (status) fetch.mockResolvedValueOnce(reply({detail: 'unavailable'}, status));
        else fetch.mockRejectedValueOnce(new TypeError('network unavailable'));
        fetch.mockResolvedValueOnce(reply(run('running')));
        const {page} = pageWith(fetch);
        await page.pickRun(project, 'run');
        await vi.advanceTimersByTimeAsync(5000);
        expect(page.activeRun.project.state).toBe('running');
    });

    it.each([401, 404])('does not endlessly retry permanent HTTP %s', async status => {
        const fetch = vi.fn().mockResolvedValue(reply({detail: 'unavailable'}, status));
        const {page} = pageWith(fetch);
        await page.pickRun(project, 'run');
        await vi.advanceTimersByTimeAsync(20000);
        expect(fetch).toHaveBeenCalledTimes(1);
    });

    it('refreshes a series after an already-running resume is rejected, then follows recovery', async () => {
        const fetch = vi.fn()
            .mockResolvedValueOnce(reply({detail: 'already running'}, 400))
            .mockResolvedValueOnce(reply(series('interrupted', {auto_resume: true})))
            .mockRejectedValueOnce(new TypeError('network unavailable'))
            .mockResolvedValueOnce(reply(series('running')))
            .mockResolvedValueOnce(reply(series('done')));
        const {page} = pageWith(fetch);
        await page.resumeSeries(project, series('interrupted'));
        await vi.advanceTimersByTimeAsync(1000);
        expect(page.seriesOf(project).state).toBe('interrupted');
        await vi.advanceTimersByTimeAsync(7000);
        expect(page.seriesOf(project).state).toBe('running');
        await vi.advanceTimersByTimeAsync(2000);
        expect(page.seriesOf(project).state).toBe('done');
        await vi.advanceTimersByTimeAsync(10000);
        expect(fetch).toHaveBeenCalledTimes(5);
    });

    it('refreshes the actual run after a rejected resume', async () => {
        const fetch = vi.fn()
            .mockResolvedValueOnce(reply({detail: 'already running'}, 400))
            .mockResolvedValueOnce(reply(run('running')));
        const {page} = pageWith(fetch);
        await page.resumeRun(project, run('interrupted'));
        await vi.advanceTimersByTimeAsync(1000);
        expect(page.activeRun.project.state).toBe('running');
    });

    it('does not keep polling after leaving the page', async () => {
        const fetch = vi.fn().mockResolvedValue(reply(run('interrupted', {auto_resume: true})));
        const {page, unmount} = pageWith(fetch);
        await page.pickRun(project, 'run');
        unmount();
        await vi.advanceTimersByTimeAsync(10000);
        expect(fetch).toHaveBeenCalledTimes(1);
    });

    it('does not apply or retry a response for the previous object', async () => {
        let reject;
        const fetch = vi.fn().mockImplementation(() => new Promise((_resolve, fail) => { reject = fail; }));
        const {page} = pageWith(fetch);
        const pending = page.pickRun(project, 'run');
        page.objectId.value = 'other';
        reject(new TypeError('network unavailable'));
        await pending;
        await vi.advanceTimersByTimeAsync(10000);
        expect(fetch).toHaveBeenCalledTimes(1);
        expect(page.error.value).toBe('');
    });
});

describe('«Описать изменения ИИ» по всем соседним переходам', () => {
    const p = {...project, versions: ['v1', 'v2', 'v3']};
    const ser = series('done', {pairs: [{a: 'v1', b: 'v2', run_id: 'r1', run_state: 'done'}, {a: 'v2', b: 'v3', run_id: 'r2', run_state: 'done'}]});
    const ai = (state, done) => ({state, pairs: [], total: {sheets: 4, sheets_done: done, calls: done, input_tokens: 1, output_tokens: 1}});

    it('оценка без запуска, запуск только с согласием, ход опрашивается до конца', async () => {
        const fetch = vi.fn()
            .mockResolvedValueOnce(reply(ser))                                   // «Досчитать» серию
            .mockResolvedValueOnce(reply(ser))                                   // обновление серии
            .mockResolvedValueOnce(reply({state: 'none', pairs: []}))            // ИИ-описания ещё нет
            .mockResolvedValueOnce(reply({pairs: [], sheets: 4, enabled: true})) // оценка
            .mockResolvedValueOnce(reply(ai('running', 0)))                      // запуск
            .mockResolvedValueOnce(reply(ai('running', 2)))
            .mockResolvedValueOnce(reply(ai('done', 4)));
        const {page} = pageWith(fetch);
        await page.resumeSeries(p, ser);
        await vi.advanceTimersByTimeAsync(1000);
        expect(page.aiSeriesReady(p)).toBe(true);
        await page.describeAll(p);
        expect(fetch.mock.calls[3][0]).toContain('/series/series/ai-sheet-changes/estimate');
        expect(page.aiSeriesEst[p.project_id].sheets).toBe(4);
        page.aiSeriesConsent[p.project_id] = true;
        await page.startAiSeries(p);
        const [url, opts] = fetch.mock.calls[4];
        expect(url).toMatch(/ai-sheet-changes$/);
        expect(opts.method).toBe('POST');
        expect(JSON.parse(opts.body)).toEqual({consent: true});
        expect(page.aiSeriesEst[p.project_id]).toBe(null);
        await vi.advanceTimersByTimeAsync(2000);
        expect(page.aiSeriesOf(p).total.sheets_done).toBe(2);
        await vi.advanceTimersByTimeAsync(3000);
        expect(page.aiSeriesOf(p).state).toBe('done');
        await vi.advanceTimersByTimeAsync(10000);
        expect(fetch).toHaveBeenCalledTimes(7);                                  // после «готово» опрос прекращён
    });
});

describe('щелчок по листу матрицы', () => {
    const versions = ['v1', 'v2', 'v3'].map(id => ({version_id: id, sha256: 'sha-' + id, user_label: id.toUpperCase()}));
    const row = {row_id: 'row', display_title: 'План', cells: {}};
    const setup = fetch => {
        const {page} = pageWith(fetch);
        page.matrices.project = {state: 'ready', versions};
        page.runs.project = [{run_id: 'r23', scope: 'all', state: 'done', a: {version_id: 'v2'}, b: {version_id: 'v3'}}];
        return page;
    };
    const done = {run_id: 'r23', scope: 'all', state: 'done', a: {version_id: 'v2'}, b: {version_id: 'v3'},
        items: [{row_id: 'row', spid: 's1', kind: 'compare', status: 'done'}]};

    it('лист третьего столбца: пара V2 → V3 и открывается сравнение этого листа', async () => {
        const page = setup(vi.fn().mockResolvedValue(reply(done)));
        await page.clickCell(project, row, versions[2], {page: 5}, {});
        expect(page.pairs.project).toEqual({a: 'v2', b: 'v3'});
        expect(page.viewer.open).toBe(true);
        expect(page.viewer.spid).toBe('s1');
    });

    it('лист первого столбца остаётся A; непосчитанная пара — лист открывается сам, с причиной', async () => {
        const fetch = vi.fn();
        const page = setup(fetch);
        await page.clickCell(project, row, versions[0], {page: 2}, {});
        expect(page.pairs.project).toEqual({a: 'v1', b: 'v2'});
        expect(fetch).not.toHaveBeenCalled();
        expect(page.viewer.open).toBe(false);
        expect(page.sheetView.open).toBe(true);
        expect(page.sheetView.url).toContain('/sources/sha-v1/pages/2/thumb?w=2000');
        expect(page.sheetView.note).toContain('ещё не посчитана');
    });

    it('лист без пары в соседней версии открывается сам; Shift+щелчок — прежний ручной выбор', async () => {
        const page = setup(vi.fn().mockResolvedValue(reply({...done, items: [{row_id: 'row', kind: 'added', status: 'done'}]})));
        await page.clickCell(project, row, versions[2], {page: 7}, {});
        expect(page.sheetView.open).toBe(true);
        expect(page.sheetView.note).toContain('нет пары в версии V2');
        page.sheetView.open = false;
        await page.clickCell(project, row, versions[1], {page: 3}, {shiftKey: true});
        expect(page.pending.project).toEqual({row: 'row', page: 3});
        expect(page.sheetView.open).toBe(false);
    });
});
