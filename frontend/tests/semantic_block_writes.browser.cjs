/* Browser acceptance of writing from stage 2 «Смысловые блоки» against the stand, never :8081.
   Start a FRESH stand with writes for every run (the test appends Human Mapping rows to the stand's copy;
   the source root is only read, the run a631b49a of p290a06df79 must have no Human Mapping history yet):
     python scripts/stage_block_mapping_smoke_server.py --copy-from comparison \
         --pairs e6fc8a2725eb4a67/p290a06df79 e6fc8a2725eb4a67/p11ad4a09d9 --port 8995 --writes
   and pass the printed «Stand state: <dir>» as SMOKE_STATE:
     SMOKE_STATE=<dir> NODE_PATH=<dir with playwright> node frontend/tests/semantic_block_writes.browser.cjs
   SMOKE_MODE=flag-off runs only the flag check against a stand started WITHOUT --writes.
   Every write goes through the UI (block clicks, the link tools, the preview); nothing is posted by the test. */
const {chromium} = require('playwright');
const assert = require('node:assert/strict'), crypto = require('node:crypto'), fs = require('node:fs'), path = require('node:path');

const base = process.env.SMOKE_BASE || 'http://127.0.0.1:8995';
const origin = new URL(base).origin;
assert(['127.0.0.1', 'localhost'].includes(new URL(base).hostname) && new URL(base).port !== '8081',
    'The block mapping acceptance runs only against the loopback stand');
const mode = process.env.SMOKE_MODE || 'writes';
assert(['writes', 'flag-off'].includes(mode), 'SMOKE_MODE is writes or flag-off');
const out = process.env.SMOKE_OUTPUT || '/home/coder/auditmanager/build-tmp/sbm-writes-browser/' + mode;
fs.mkdirSync(out, {recursive: true});
const SID = 'e6fc8a2725eb4a67', PID = 'p290a06df79', RUN = 'a631b49aaaac4db0af66a495c155c629', OID = '4f3e5916';
const BM = `/api/stage-comparison/sessions/${SID}/pairs/${PID}/block-mapping`;
const HM = `/api/human-mapping/objects/${OID}/comparisons/${PID}`;
const SCOPE = `session_id=${SID}&run_id=${RUN}`;
const B = {g20: 'blk_d2ebac1ee4424ad596bcbe459b211e88', t10: 'blk_55bd55313fe8409cbb6cfd34b9348536',
    t9: 'blk_ab9a0461cbe44d2395af7af13d3d9dd3', r6old: 'blk_a090e903c2fa423082fbfb00df70daad'};
const key = (o, n) => o + '|' + n;

// The stand's copy of the run: only Human Mapping history may change there (C12).
const state = process.env.SMOKE_STATE;
assert(state && fs.existsSync(path.join(state, 'comparison')), 'SMOKE_STATE must be the «Stand state:» directory');
const production = path.join(state, 'comparison/sessions', SID, 'pairs', PID, 'production');
const runDir = path.join(production, 'runs', RUN);
const HISTORY_FILES = ['human_mapping/human_block_link_edits.jsonl', 'human_mapping/human_block_link_edits.jsonl.lock',
    'human_mapping/reviews.jsonl', 'human_mapping/reviews.jsonl.lock'];
function fingerprint() {
    const files = {};
    (function walk(dir) {
        for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
            const full = path.join(dir, entry.name);
            if (entry.isDirectory()) walk(full);
            else files[path.relative(runDir, full)] = crypto.createHash('sha256').update(fs.readFileSync(full)).digest('hex');
        }
    })(runDir);
    const current = crypto.createHash('sha256').update(fs.readFileSync(path.join(production, 'current_run.json'))).digest('hex');
    return {files, current};
}
function changedFiles(before, after) {
    return [...new Set([...Object.keys(before.files), ...Object.keys(after.files)])]
        .filter(f => before.files[f] !== after.files[f]).sort();
}

const checks = [], requests = [], errors = [], external = [];
let browser, context, page;
const save = (status, error) => fs.writeFileSync(path.join(out, 'browser-results.json'), JSON.stringify(
    {status, mode, error: error ? String(error.stack || error) : null, base, state, checks, errors, external,
        requests: requests.map(r => r.method + ' ' + r.path)}, null, 2));
async function check(name, fn) { await fn(); checks.push({name, status: 'PASS'}); console.log('PASS ' + name); save('RUNNING'); }
const shot = name => page.screenshot({path: path.join(out, name + '.png'), animations: 'disabled', fullPage: true});
const norm = text => text.replace(/\s+/g, ' ');
const workspaceText = async () => norm(await page.locator('.sbm-workspace').innerText());
// Human Mapping writes only: the portal shell itself posts /api/stage-comparison/sessions on start (pre-existing).
const posts = () => requests.filter(r => r.method === 'POST' && r.path.startsWith('/api/human-mapping/'));
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(fn, what, ms = 20000) {
    const started = Date.now();
    while (!(await fn())) {
        if (Date.now() - started > ms) throw new Error('Timed out: ' + what);
        await sleep(50);
    }
}
const waitText = text => page.waitForFunction(t => (document.querySelector('.sbm-workspace')?.innerText || '')
    .replace(/\s+/g, ' ').includes(t), text);

async function openCatalogPair(label) {
    await page.getByRole('button', {name: '1. Загрузка документации', exact: true}).click();
    await page.locator('tr', {hasText: label}).getByRole('button', {name: 'Открыть', exact: true}).first().click();
    await page.locator('#pc-catalog-focus', {hasText: label}).waitFor();
    await page.getByRole('button', {name: '2. Сопоставление листов', exact: true}).click();
}
async function openChip(prefix) {
    const row = page.locator('.sc-sheet-map__row').filter({has: page.locator('.sc-sheet-map__semantic', {hasText: prefix})}).first();
    await row.locator('.sc-sheet-map__semantic').click();
    await page.locator('.sbm-workspace').waitFor();
}
async function rastersLoaded() {
    await page.waitForFunction(() => {
        const imgs = [...document.querySelectorAll('.sbm-page__img')];
        return imgs.length > 0 && imgs.every(i => i.complete && i.naturalWidth > 0);
    });
}
async function withStatus(patch, fn) {
    await page.route('**' + BM + '/status', async route => {
        const response = await route.fetch();
        const body = await response.json();
        patch(body, body.runs.find(r => r.run_id === RUN));
        await route.fulfill({response, json: body});
    });
    try { await fn(); } finally { await page.unroute('**' + BM + '/status'); }
}

// ── UI steps ──────────────────────────────────────────────────────────────
const ws = () => page.locator('.sbm-workspace');
const block = (side, id) => ws().locator(`[data-sbm-side="${side}"][data-sbm-block="${id}"]`).first();
const writeButton = name => ws().locator(`[data-sbm-write="${name}"]`);
async function pick(side, id) {
    await block(side, id).click();
    await until(async () => /\bis-selected\b/.test(await block(side, id).getAttribute('class')), `${side} ${id} selected`);
}
async function editOn() {
    if (await writeButton('edit-toggle').getAttribute('aria-pressed') !== 'true') await writeButton('edit-toggle').click();
    await until(async () => await writeButton('edit-toggle').getAttribute('aria-pressed') === 'true', 'edit mode');
}
async function wholeRegion(id) {
    await ws().locator('.sbm-lens button', {hasText: 'Эта пара'}).click();
    await ws().locator('.sbm-shelves .sbm-region', {hasText: new RegExp('(^|◆ )' + id + ' ')}).first().click();
    await ws().getByRole('button', {name: 'Весь регион', exact: true}).click();
    await ws().locator('.sbm-lens button.is-active', {hasText: 'Весь регион'}).waitFor();
    await ws().locator('.sbm-decision strong', {hasText: 'Решение по региону ' + id}).waitFor();
}
// One UI action that must end in `n` POSTs, then the reconciling GET reviews + GET block-links, then a settled status.
async function write(action, n, what) {
    const mark = requests.length, before = posts().length;
    await action();
    await until(() => posts().length >= before + n, `${what}: ${n} POST`);
    await page.waitForFunction(() => {
        const el = document.querySelector('.sbm-write-status');
        return el && !/\bis-(writing|checking)\b/.test(el.className);
    });
    const list = requests.slice(mark);
    assert.equal(list.filter(r => r.method === 'POST').length, n, what);
    const last = list.map(r => r.method).lastIndexOf('POST');
    for (const kind of ['reviews', 'block-links']) {
        assert(list.slice(last + 1).some(r => r.method === 'GET' && r.path === `${HM}/${kind}?${SCOPE}`), `${what}: reread ${kind}`);
    }
    const status = norm(await page.locator('.sbm-write-status').innerText());
    assert(status.includes('✓ Записано в историю прогона a631b49a'), `${what}: ${status}`);
    return status;
}
async function connectOne(oldId, newId, {region = '', hint = ''} = {}) {
    await pick('OLD', oldId);
    await pick('NEW', newId);
    await editOn();
    if (hint) assert.equal(norm(await ws().locator('.sbm-hint-inline').innerText()), hint);
    await write(async () => {
        await writeButton('connect-one').click();
        if (region) {
            await page.locator('.sbm-dialog').waitFor();
            await page.locator(`.sbm-dialog [data-sbm-write="region-choice"][data-sbm-region="${region}"]`).click();
        }
    }, 1, `1→1 ${region || hint}`);
}
async function edgeState(k) {
    const item = ws().locator(`.sbm-links-list__item[data-sbm-link="${k}"]`);
    await item.waitFor();
    return norm(await item.innerText());
}
async function commitPreview(title, what) {
    const dialog = page.locator('.sbm-dialog');
    await dialog.waitFor();
    const text = norm(await dialog.innerText());
    assert(text.startsWith(title), text);
    // The safe button has the focus; the commit is an explicit click.
    assert.equal(await page.evaluate(() => document.activeElement && document.activeElement.dataset.sbmWrite), 'dialog-cancel');
    const commit = page.locator('.sbm-dialog [data-sbm-write="dialog-commit"]');
    const label = (await commit.innerText()).trim();
    await write(() => commit.click(), 1, what);
    await dialog.waitFor({state: 'detached'});
    return {text, label};
}

async function flagOffScenario(label) {
    await check(`8 (${label}): no write controls, state L, nothing posted`, async () => {
        await waitText('Редактирование смысловых связей в этом разделе ещё не включено. Для правки откройте Human Mapping ↗');
        assert.equal(await ws().locator('[data-sbm-write]').count(), 0);
        assert.equal(await ws().locator('.sbm-write-status, .sbm-dialog, .sbm-tools').count(), 0);
        // Selecting a link on a read-only run offers no decision either.
        const item = ws().locator('.sbm-links-list__item').first();
        if (await item.count()) {
            await item.click();
            assert.equal(await ws().locator('[data-sbm-write]').count(), 0);
        }
        await shot('08-flag-off-' + label);
    });
}

(async () => {
    const before = fingerprint();
    browser = await chromium.launch({headless: true, executablePath: process.env.CHROME_BIN || '/opt/google/chrome/chrome',
        args: ['--no-sandbox']});
    context = await browser.newContext({viewport: {width: 1600, height: 1100}, serviceWorkers: 'block'});
    context.setDefaultTimeout(20000);
    await context.route('**/*', route => {
        const url = new URL(route.request().url());
        if (url.origin !== origin) { external.push(url.href); return route.abort(); }
        return route.continue();
    });
    context.on('request', r => { const u = new URL(r.url()); requests.push({method: r.method(), path: u.pathname + u.search}); });
    page = await context.newPage();
    page.on('pageerror', e => errors.push(e.message));

    const status = await (await page.request.get(base + BM + '/status')).json();
    const run = status.runs.find(r => r.run_id === RUN);

    if (mode === 'flag-off') {
        await check('8: the stand without --writes reports the flag off', async () => {
            assert.equal(status.capabilities.block_mapping_writes, false);
            assert.equal(run.writable, false);
            assert.equal(run.write_block_reason, 'WRITES_DISABLED');
        });
        await page.goto(base + '/#/stage-comparison');
        await page.locator('tr', {hasText: 'ИОС2.1'}).first().waitFor();
        await openCatalogPair('ИОС2.1');
        await page.locator('.sc-sheet-map__semantic').first().waitFor();
        await openChip('◇6');
        await rastersLoaded();
        await flagOffScenario('stand');
        await check('8: no POST, the run copy unchanged, no external request, no page error', async () => {
            assert.deepEqual(posts(), []);
            assert.deepEqual(changedFiles(before, fingerprint()), []);
            assert.deepEqual(external.filter(u => !/^https:\/\/fonts\.(googleapis|gstatic)\.com\//.test(u)), []);
            assert.deepEqual(errors, []);
        });
        save('PASS');
        await browser.close();
        return;
    }

    await check('1: status allows writing; the run copy is fingerprinted before any action', async () => {
        assert.equal(status.capabilities.block_mapping_writes, true);
        assert.equal(run.writable, true);
        assert.equal(run.write_block_reason, null);
        assert.deepEqual(run.history, {reviews: 0, block_link_events: 0, poisoned: false, poison_codes: []});
        assert(Object.keys(before.files).length > 100, 'run files: ' + Object.keys(before.files).length);
        for (const f of HISTORY_FILES) assert(!(f in before.files), f + ' must not exist yet');
    });

    await page.goto(base + '/#/stage-comparison');
    await page.locator('tr', {hasText: 'ИОС2.1'}).first().waitFor();
    await openCatalogPair('ИОС2.1');
    await page.locator('.sc-sheet-map__semantic').first().waitFor();
    await openChip('◇6');
    await rastersLoaded();

    await check('1: write controls are shown, state L is not', async () => {
        for (const name of ['edit-toggle', 'connect-one', 'connect-spoke', 'connect-cartesian'])
            assert(await writeButton(name).isVisible(), name);
        assert(!(await workspaceText()).includes('Редактирование смысловых связей в этом разделе ещё не включено.'));
        assert.equal(posts().length, 0);
    });

    await check('2: A-R016 — 1→1 through the UI draws a dashed human link, «Подтвердить…» via the preview puts ✓ on it', async () => {
        await connectOne(B.g20, B.t10, {hint: 'Связь будет создана в регионе A-R016'});
        const k = key(B.g20, B.t10);
        assert((await edgeState(k)).includes('не проверена'), await edgeState(k));
        const line = ws().locator('.sbm-edge.sbm-edge--unreviewed.sbm-edge--human .sbm-edge__line').first();
        await line.waitFor();
        assert.notEqual(await line.evaluate(el => getComputedStyle(el).strokeDasharray), 'none');
        const status = norm(await page.locator('.sbm-write-status').innerText());
        assert(status.includes('Связь записана, но ещё не закреплена: у региона нет решения'), status);
        await shot('02a-r016-link');
        await writeButton('region-confirm').click();
        const {text, label} = await commitPreview('Подтвердить связи региона A-R016', 'A-R016 confirm');
        assert(text.includes('Будут закреплены как якоря (1): OLD графика стр. 20 → NEW таблица стр. 10'), text);
        assert.equal(label, 'Записать решение');
        await ws().locator('.sbm-edge.sbm-edge--anchor .sbm-edge__glyph', {hasText: '✓'}).first().waitFor();
        assert((await edgeState(k)).startsWith('✓'), await edgeState(k));
        await shot('02b-r016-anchor');
    });

    await check('3: A-R014 «Весь регион» — 1→1 (region chosen in the dialog), «Отклонить именно эту связь» → ⊘ on the edge only', async () => {
        await wholeRegion('A-R014');
        await block('NEW', B.t9).waitFor();
        await connectOne(B.g20, B.t9, {region: 'A-R014'});
        const k = key(B.g20, B.t9);
        await ws().locator(`.sbm-links-list__item[data-sbm-link="${k}"]`).click();
        await writeButton('link-reject').click();
        const {text} = await commitPreview('Отклонить связи региона A-R014', 'A-R014 reject link');
        assert(text.includes('Будут запрещены именно эти связи (1): OLD графика стр. 20 → NEW таблица стр. 9'), text);
        assert(text.includes('Запрещена будет только связь OLD графика стр. 20 → NEW таблица стр. 9.'), text);
        await ws().locator('.sbm-edge.sbm-edge--forbidden .sbm-edge__glyph', {hasText: '⊘'}).first().waitFor();
        assert((await edgeState(k)).startsWith('⊘'), await edgeState(k));
        for (const [side, id] of [['OLD', B.g20], ['NEW', B.t9]]) {
            const cls = await block(side, id).getAttribute('class');
            assert(!/reject|forbid|refus|denied/i.test(cls), `${side} ${id}: ${cls}`);
        }
        await shot('03a-r014-forbidden');
    });

    await check('3: «Проверка для нового анализа» — the bridge builds on the stage 2 events (anchors 1, rejections 1)', async () => {
        const mark = requests.length;
        await ws().getByRole('button', {name: 'Проверка для нового анализа', exact: true}).click();
        await waitText('Снимок для нового анализа собирается: якорей 1 · запретов 1.');
        assert.equal(requests.slice(mark).filter(r => r.path === BM + '/runs/' + RUN + '/bridge-check').length, 1);
        await shot('03b-bridge-ok');
    });

    await check('4: A-R006 — a link left uncertain: «Оставить неуверенной» → ? on the edge', async () => {
        await wholeRegion('A-R006');
        await block('OLD', B.r6old).waitFor();
        await connectOne(B.r6old, B.t10, {hint: 'Связь будет создана в регионе A-R006'});
        const k = key(B.r6old, B.t10);
        await ws().locator(`.sbm-links-list__item[data-sbm-link="${k}"]`).click();
        await writeButton('link-uncertain').click();
        await commitPreview('Не уверен: регион A-R006', 'A-R006 uncertain');
        await ws().locator('.sbm-edge.sbm-edge--uncertain .sbm-edge__glyph', {hasText: '?'}).first().waitFor();
        assert((await edgeState(k)).startsWith('?'), await edgeState(k));
        await shot('04-r006-uncertain');
    });

    await check('5: D-14 — A-R010 the same blocks, 1→1 and «Подтвердить…»: conflict dialog, «Записать всё равно», M2, the bridge refuses', async () => {
        await wholeRegion('A-R010');
        await block('NEW', B.t9).waitFor();
        await connectOne(B.g20, B.t9, {region: 'A-R010'});
        await writeButton('region-confirm').click();
        await page.locator('.sbm-dialog__conflict').waitFor();
        assert.deepEqual((await page.locator('.sbm-dialog__conflict').allInnerTexts()).map(norm),
            ['Связь OLD графика стр. 20 → NEW таблица стр. 9 уже запрещена в регионе A-R014. Если закрепить её в A-R010, '
                + 'снимок для нового анализа не соберётся, пока одно из решений не будет заменено.']);
        await shot('05a-conflict-dialog');
        const {label} = await commitPreview('Подтвердить связи региона A-R010', 'A-R010 confirm despite conflict');
        assert.equal(label, 'Записать всё равно');
        await waitText('Одна и та же связь закреплена в A-R010 и запрещена в A-R014 — снимок для нового анализа не соберётся.');
        await ws().locator('.sbm-edge.sbm-edge--cross_region_conflict .sbm-edge__glyph', {hasText: '⚠'}).first().waitFor();
        const mark = requests.length;
        await ws().getByRole('button', {name: 'Проверка для нового анализа', exact: true}).click();
        await waitText('Не собирается: одна и та же связь закреплена в одном регионе и запрещена в другом.');
        assert.equal(requests.slice(mark).filter(r => r.path === BM + '/runs/' + RUN + '/bridge-check').length, 1);
        await shot('05b-m2-bridge');
    });

    await check('6: in the run copy only the Human Mapping history changed (4 files), current_run.json unchanged', async () => {
        const after = fingerprint();
        assert.deepEqual(changedFiles(before, after), [...HISTORY_FILES].sort());
        assert.equal(after.current, before.current);
    });

    await check('7: stored events are those of the classic HM page, written with the explicit scope', async () => {
        const all = posts();
        assert.equal(all.length, 8);
        assert.deepEqual(requests.filter(r => !['GET', 'HEAD'].includes(r.method) && !r.path.startsWith('/api/human-mapping/'))
            .map(r => r.method + ' ' + r.path), ['POST /api/stage-comparison/sessions']);
        for (const p of all) assert.match(p.path, new RegExp(`^${HM}/(block-links|reviews)\\?${SCOPE}$`));
        const edits = await (await page.request.get(`${base}${HM}/block-links?${SCOPE}`)).json();
        const reviews = await (await page.request.get(`${base}${HM}/reviews?${SCOPE}`)).json();
        const common = {pair: PID, pair_key: PID, comparison_id: PID, object_id: OID, run_id: RUN, result_id: null, reviewer_source: 'HUMAN'};
        const uuid = /^human:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
        assert.deepEqual(edits.map(e => [e.region_id, e.old_block_id, e.new_block_id]),
            [['A-R016', B.g20, B.t10], ['A-R014', B.g20, B.t9], ['A-R006', B.r6old, B.t10], ['A-R010', B.g20, B.t9]]);
        for (const e of edits) {
            assert.deepEqual(Object.keys(e).sort(), ['comment', 'comparison_id', 'event_id', 'event_type', 'link_id', 'new_block_id',
                'object_id', 'old_block_id', 'pair', 'pair_key', 'previous_link_id', 'previous_new_block_id', 'previous_old_block_id',
                'region_id', 'result_id', 'reviewer_source', 'run_id', 'timestamp']);
            assert.deepEqual({...e, event_id: 0, timestamp: 0, link_id: 0, region_id: 0, old_block_id: 0, new_block_id: 0},
                {...common, event_id: 0, timestamp: 0, link_id: 0, region_id: 0, old_block_id: 0, new_block_id: 0,
                    event_type: 'ADD_BLOCK_LINK', previous_old_block_id: null, previous_new_block_id: null, previous_link_id: null, comment: ''});
            assert.match(e.link_id, uuid);
        }
        assert.equal(new Set(edits.map(e => e.link_id)).size, 4);
        const region = id => {
            const ui = JSON.parse(fs.readFileSync(path.join(runDir, 'human_mapping/ui_data.json'), 'utf8')).regions.find(r => r.id === id);
            return {old: ui.old_blocks.map(b => b.id), new: ui.new_blocks.map(b => b.id)};
        };
        assert.deepEqual(reviews.map(r => [r.region_id, r.status, r.old_block_ids, r.new_block_ids]), [
            ['A-R016', 'HUMAN_CONFIRMED', region('A-R016').old, region('A-R016').new],   // nothing selected → all members (HM save)
            ['A-R014', 'HUMAN_REJECTED', [B.g20], [B.t9]],
            ['A-R006', 'HUMAN_UNCERTAIN', [B.r6old], [B.t10]],
            ['A-R010', 'HUMAN_CONFIRMED', region('A-R010').old, region('A-R010').new],
        ]);
        for (const r of reviews) {
            assert.deepEqual(Object.keys(r).sort(), ['comment', 'comparison_id', 'new_block_ids', 'object_id', 'old_block_ids', 'pair',
                'pair_key', 'region_id', 'result_id', 'review_id', 'reviewer_source', 'run_id', 'status', 'supersedes_review_id', 'timestamp']);
            assert.deepEqual({...r, review_id: 0, timestamp: 0, region_id: 0, status: 0, old_block_ids: 0, new_block_ids: 0},
                {...common, review_id: 0, timestamp: 0, region_id: 0, status: 0, old_block_ids: 0, new_block_ids: 0,
                    comment: '', supersedes_review_id: null});
        }
    });

    await check('8 (status route): the flag off in the status → reload shows state L without write controls', async () => {
        await withStatus((body, r) => {
            body.capabilities = {...body.capabilities, block_mapping_writes: false};
            Object.assign(r, {writable: false, write_block_reason: 'WRITES_DISABLED', write_block_reasons: ['WRITES_DISABLED']});
        }, async () => {
            const mark = posts().length;
            await page.reload();
            await page.locator('.sc-sheet-map__semantic').first().waitFor();
            await openChip('◇6');
            await rastersLoaded();
            await flagOffScenario('route');
            assert.equal(posts().length, mark);
        });
    });

    await check('9: no external request, no page error', async () => {
        // The portal shell links Google Fonts in index.html (pre-existing); the stand aborts it. Nothing else may leave.
        assert.deepEqual(external.filter(u => !/^https:\/\/fonts\.(googleapis|gstatic)\.com\//.test(u)), []);
        assert.deepEqual(errors, []);
    });
    save('PASS');
    await browser.close();
})().catch(async error => {
    save('FAIL', error);
    console.error(error);
    try { if (page) await shot('failure'); } catch (_) { /* best effort */ }
    if (browser) await browser.close();
    process.exit(1);
});
