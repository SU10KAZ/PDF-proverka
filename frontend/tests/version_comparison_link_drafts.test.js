import {afterEach, expect, it, vi} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../static/js/version-comparison.js', import.meta.url), 'utf8');
const p = {project_id: 'project'};
const v = version_id => ({version_id});
const matched = page => ({state: 'matched', page});
const missing = () => ({state: 'unmatched', candidates: [{page: 3}, {page: 4}]});
const matrix = () => ({state: 'ready', versions: [v('a'), v('b'), v('c')], edits: [], rows: [
    {row_id: 'one', anchor: ['a', 1], cells: {a: matched(1), b: missing(), c: missing()}},
    {row_id: 'two', anchor: ['a', 2], cells: {a: matched(2), b: missing(), c: missing()}},
    {row_id: 'three', anchor: ['b', 3], cells: {a: missing(), b: matched(3), c: matched(3)}},
    {row_id: 'four', anchor: ['b', 4], cells: {a: missing(), b: matched(4), c: matched(4)}},
]});
const reply = (data, status = 200) => ({ok: status < 400, status, json: async () => data});
function mount(fetch = vi.fn()) {
    vi.useFakeTimers();
    const sandbox = {fetch, FormData: class {}, setTimeout, clearTimeout,
        Vue: {ref: value => ({value}), reactive: value => value, watch() {}, onMounted() {}, onBeforeUnmount() {}}};
    sandbox.globalThis = sandbox;
    vm.runInNewContext(source, sandbox);
    let component;
    sandbox.VersionComparison.register({component: (_name, value) => {component = value;}});
    const page = component.setup();
    page.objectId.value = 'obj';
    page.matrices.project = matrix();
    page.pairs.project = {a: 'a', b: 'b'};
    return {page, fetch};
}
afterEach(() => {vi.clearAllTimers(); vi.useRealTimers();});

it('collects choices and absence without requests or removing the ready table; undo is local', async () => {
    const {page, fetch} = mount();
    const rows = () => page.matrices.project.rows;
    page.attach(p, rows()[0], v('b'), 3);
    page.markAbsent(p, rows()[1], v('c'));
    expect(fetch).not.toHaveBeenCalled();
    expect(page.matrices.project.state).toBe('ready');
    expect(rows().map(r => r.row_id)).toEqual(['one', 'two', 'three', 'four']);
    expect(rows()[0].cells.b.page).toBe(3);
    expect(rows()[2].cells.b.state).toBe('absent');
    expect(rows()[2].anchor).toEqual(['c', 3]);
    expect(rows()[1].cells.c.state).toBe('absent');
    expect(page.linkDraft(p).edits).toHaveLength(2);
    await page.undoEdit(p);
    expect(rows()[1].cells.c.state).toBe('unmatched');
    expect(rows()[0].cells.b.page).toBe(3);
    await page.undoEdit(p);
    expect(page.linkDraft(p)).toBeUndefined();
    expect(page.matrices.project).toEqual(matrix());
    expect(fetch).not.toHaveBeenCalled();
});

it('uses the remaining anchor when a moved page was the anchor of a later edited row', () => {
    const {page} = mount();
    page.attach(p, page.matrices.project.rows[0], v('b'), 3);
    page.attach(p, page.matrices.project.rows[2], v('b'), 4);
    expect(page.linkDraft(p).edits[1].to_row_anchor).toEqual(['c', 3]);
    expect(page.matrices.project.rows[2].cells.b.page).toBe(4);
});

it('applies one batch before comparison and keeps the table visible during rebuilding', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(reply({state: 'building'}))
        .mockResolvedValueOnce(reply(matrix()))
        .mockResolvedValueOnce(reply({run_id: 'run', state: 'queued'}));
    const {page} = mount(fetch);
    page.attach(p, page.matrices.project.rows[0], v('b'), 3);
    page.attach(p, page.matrices.project.rows[1], v('b'), 4);
    const running = page.startRun(p, 'all');
    await vi.advanceTimersByTimeAsync(0);
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0][0]).toMatch(/links\/batch$/);
    expect(JSON.parse(fetch.mock.calls[0][1].body).edits).toHaveLength(2);
    expect(page.matrices.project.state).toBe('ready');
    expect(page.linksBusy(p)).toBe(true);
    await page.applyLinks(p); // repeated clicks cannot send a second batch
    page.markAbsent(p, page.matrices.project.rows[0], v('c'));
    expect(page.linkDraft(p).edits).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(1000);
    await running;
    expect(fetch.mock.calls.map(c => c[0].split('/').pop())).toEqual(['batch', 'matrix', 'runs']);
    expect(page.linkDraft(p)).toBeUndefined();
    expect(page.linksBusy(p)).toBe(false);
});

it('preserves rejected edits and does not start comparison', async () => {
    const fetch = vi.fn().mockResolvedValue(reply({detail: 'server unavailable'}, 503));
    const {page} = mount(fetch);
    page.attach(p, page.matrices.project.rows[0], v('b'), 3);
    await page.startSeries(p);
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(page.linkDraft(p).edits).toHaveLength(1);
    expect(page.linksLocked(p)).toBe(false);
    expect(page.matrices.project.rows[0].cells.b.page).toBe(3);
    expect(page.error.value).toBe('server unavailable');
});

it('retries only the matrix refresh after an accepted batch and a failed refresh', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(reply({state: 'building'}))
        .mockRejectedValueOnce(new TypeError('offline')).mockResolvedValueOnce(reply(matrix()));
    const {page} = mount(fetch);
    page.attach(p, page.matrices.project.rows[0], v('b'), 3);
    const applying = page.applyLinks(p);
    await vi.advanceTimersByTimeAsync(1000);
    expect(await applying).toBe(false);
    expect(page.linkDraft(p).saved).toBe(true);
    expect(await page.applyLinks(p)).toBe(true);
    expect(fetch.mock.calls.map(c => c[0].split('/').pop())).toEqual(['batch', 'matrix', 'matrix']);
});

it('does not reload the draft when selecting the project again', async () => {
    const {page, fetch} = mount();
    page.projects.value = [p];
    page.selectedProject.value = p.project_id;
    page.attach(p, page.matrices.project.rows[0], v('b'), 3);
    page.selectProject();
    expect(fetch).not.toHaveBeenCalled();
    expect(page.matrices.project.rows[0].cells.b.page).toBe(3);
});
