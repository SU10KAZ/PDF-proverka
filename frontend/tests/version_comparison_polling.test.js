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
