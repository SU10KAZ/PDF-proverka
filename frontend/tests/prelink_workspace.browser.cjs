/* Browser acceptance of the pre-analysis prelinks in stage 2 against the stand, never :8081. 0 model calls.
   A FRESH stand per mode (it writes only into its own copy of the comparison root):
     result: python scripts/stage_block_mapping_smoke_server.py --copy-from comparison --pairs e6fc8a2725eb4a67/p290a06df79 \
                 --port 8993 --writes --prelinks --prelink-seed-dev5 e6fc8a2725eb4a67/p290a06df79
     draft:  … --port 8993 --prelinks
     off:    … --port 8993
   then: SMOKE_MODE=<mode> SMOKE_STATE=<«Stand state:» dir> NODE_PATH=<dir with playwright> \
         node frontend/tests/prelink_workspace.browser.cjs
   Every action goes through the UI; the test itself posts nothing. */
const {chromium} = require('playwright');
const assert = require('node:assert/strict'), crypto = require('node:crypto'), fs = require('node:fs'), path = require('node:path');

const base = process.env.SMOKE_BASE || 'http://127.0.0.1:8993';
const origin = new URL(base).origin;
assert(['127.0.0.1', 'localhost'].includes(new URL(base).hostname) && new URL(base).port !== '8081',
    'The prelink acceptance runs only against the loopback stand');
const mode = process.env.SMOKE_MODE || 'result';
assert(['result', 'draft', 'off'].includes(mode), 'SMOKE_MODE is result, draft or off');
const out = process.env.SMOKE_OUTPUT || '/home/coder/auditmanager/build-tmp/prelink-browser/' + mode;
fs.mkdirSync(out, {recursive: true});
const SID = 'e6fc8a2725eb4a67', PID = 'p290a06df79', RUN = 'a631b49aaaac4db0af66a495c155c629', OID = '4f3e5916';
const HM = `/api/human-mapping/objects/${OID}/comparisons/${PID}`;
const PRELINKS = `/api/stage-comparison/sessions/${SID}/pairs/${PID}/prelinks`;
const RECON = `/api/stage-comparison/sessions/${SID}/pairs/${PID}/block-mapping/runs/${RUN}/prelink-reconciliation`;
const state = process.env.SMOKE_STATE;
assert(state && fs.existsSync(path.join(state, 'comparison')), 'SMOKE_STATE must be the «Stand state:» directory');
const runDir = path.join(state, 'comparison/sessions', SID, 'pairs', PID, 'production', 'runs', RUN);
function fingerprint(dir, skip = () => false) {
    const files = {};
    (function walk(d) {
        for (const entry of fs.readdirSync(d, {withFileTypes: true})) {
            const full = path.join(d, entry.name);
            if (skip(path.relative(dir, full))) continue;
            if (entry.isDirectory()) walk(full);
            else files[path.relative(dir, full)] = crypto.createHash('sha256').update(fs.readFileSync(full)).digest('hex');
        }
    })(dir);
    return files;
}

const checks = [], requests = [], errors = [], external = [];
let browser, page;
const save = (status, error) => fs.writeFileSync(path.join(out, 'browser-results.json'), JSON.stringify(
    {status, mode, error: error ? String(error.stack || error) : null, base, state, checks, errors, external,
        requests: requests.map(r => r.method + ' ' + r.path)}, null, 2));
async function check(name, fn) { await fn(); checks.push({name, status: 'PASS'}); console.log('PASS ' + name); save('RUNNING'); }
const shot = name => page.screenshot({path: path.join(out, name + '.png'), animations: 'disabled', fullPage: true});
const norm = text => text.replace(/\s+/g, ' ');
const ws = () => page.locator('.sbm-workspace');
const workspaceText = async () => norm(await ws().innerText());
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(fn, what, ms = 20000) {
    const started = Date.now();
    while (!(await fn())) {
        if (Date.now() - started > ms) throw new Error('Timed out: ' + what);
        await sleep(50);
    }
}
const hmPosts = () => requests.filter(r => r.method === 'POST' && r.path.startsWith('/api/human-mapping/'));
const prelinkWrites = () => requests.filter(r => r.method !== 'GET' && r.path.startsWith(PRELINKS));
const prelinkReads = () => requests.filter(r => r.path.startsWith(PRELINKS) || r.path.includes('/prelink-reconciliation'));
const pl = name => ws().locator(`[data-sbm-prelink="${name}"]`);

async function openPair() {
    await page.goto(base + '/#/stage-comparison');
    await page.locator('tr', {hasText: 'ИОС2.1'}).first().waitFor();
    await page.getByRole('button', {name: '1. Загрузка документации', exact: true}).click();
    await page.locator('tr', {hasText: 'ИОС2.1'}).getByRole('button', {name: 'Открыть', exact: true}).first().click();
    await page.locator('#pc-catalog-focus', {hasText: 'ИОС2.1'}).waitFor();
    await page.getByRole('button', {name: '2. Сопоставление листов', exact: true}).click();
    await page.locator('.sc-sheet-map__semantic').first().waitFor();
    const row = page.locator('.sc-sheet-map__row').filter({has: page.locator('.sc-sheet-map__semantic', {hasText: '◇6'})}).first();
    await row.locator('.sc-sheet-map__semantic').click();
    await ws().waitFor();
    await page.waitForFunction(() => {
        const imgs = [...document.querySelectorAll('.sbm-page__img')];
        return imgs.length > 0 && imgs.every(i => i.complete && i.naturalWidth > 0);
    });
}
async function chooseBinding(label) {
    await ws().locator('.sbm-binding summary').click();
    await ws().locator('.sbm-binding .sbm-menu button', {hasText: label}).first().click();
}
async function openFromList(id) {
    await ws().locator('.sbm-prelink-list summary').click();
    await ws().locator(`.sbm-prelink-list [data-sbm-prelink-item="${id}"]`).click();
    await pl('card').waitFor();
}

async function resultMode() {
    const recon = await (await page.request.get(base + RECON)).json();
    const by = Object.fromEntries(recon.items.map(i => [i.label, i]));
    const runBefore = fingerprint(runDir, rel => rel.startsWith('human_mapping'));
    await openPair();
    await check('R1: the run of the analysis shows the reconciliation summary and says the AI did not see the links', async () => {
        await pl('summary').waitFor();
        const text = await workspaceText();
        assert(text.includes('Ваши связи из анализа a631b49a (ред. 9): ✓ совпало 3 · ◐ частично 3 · ⚠ расходится 3 · ? не определено 0 · ⌀ не сверялись 0'), text);
        assert(text.includes('Связи не влияли на анализ — ИИ их не видел.'), text);
        assert.equal(await ws().locator('.sbm-prelink').count(), 0, 'the layer is off by default');
        await shot('r1-summary');
    });
    await check('R2: a wrong link (W1 = PL-7) is «⚠ Расходится», cannot be confirmed, «Согласиться с ИИ» writes nothing', async () => {
        await openFromList(by['PL-7'].prelink_id);
        await until(async () => (await pl('state').innerText()).includes('⚠ Расходится'), 'W1 state');
        assert.equal(await pl('confirm').count(), 0);
        await until(async () => (await ws().locator(`[data-sbm-prelink-line="${by['PL-7'].prelink_id}"].is-ghost.is-selected`).count()) === 1,
            'the selected ghost line of W1');
        const before = hmPosts().length;
        await pl('agree').click();
        await until(async () => (await workspaceText()).includes('Вы согласились с ИИ. В Human Mapping ничего не записано.'), 'agree note');
        assert.equal(hmPosts().length, before);
        await shot('r2-w1-conflict');
    });
    await check('R3: an N↔N link (C5 = PL-5) offers no Cartesian pairs', async () => {
        await openFromList(by['PL-5'].prelink_id);
        const card = norm(await ws().locator('.sbm-inspector').innerText());
        assert(card.startsWith('PL-5 · N↔N · из анализа a631b49a ✓ Совпало') && card.includes('Группа N↔N не задаёт пар блоков.'), card);
        assert.equal(await pl('status').count(), 0, 'the note of another link is not carried over');
        assert.equal(await pl('confirm').count(), 0, 'nothing to promote without pairs');
        const line = ws().locator(`[data-sbm-prelink-line="${by['PL-5'].prelink_id}"]`);
        await until(async () => (await line.locator('.sbm-prelink__node').count()) === 1, 'one node');
        assert.equal(await line.locator('.sbm-prelink__line').count(), 4, 'spokes, not 2 × 2 pairs');
        await shot('r3-c5-group');
    });
    await check('R4: «Подтвердить» a matched link (C1 = PL-1) → preview → 2 ordinary HM POSTs with the server link id', async () => {
        await openFromList(by['PL-1'].prelink_id);
        assert((await pl('state').innerText()).includes('✓ Совпало'));
        const before = hmPosts().length;
        await pl('confirm').click();
        const dialog = page.locator('.sbm-dialog');
        await dialog.waitFor();
        assert(norm(await dialog.innerText()).startsWith('Подтвердить связи региона A-R007'), norm(await dialog.innerText()));
        assert.equal(hmPosts().length, before, 'nothing is written before the explicit commit');
        await page.locator('.sbm-dialog [data-sbm-write="dialog-commit"]').click();
        await until(() => hmPosts().length >= before + 2, '2 POST');
        await until(async () => (await workspaceText()).includes('перенесено в A-R007'), 'promoted mark');
        const edits = fs.readFileSync(path.join(runDir, 'human_mapping/human_block_link_edits.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
        const reviews = fs.readFileSync(path.join(runDir, 'human_mapping/reviews.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
        assert.equal(edits.length, 1);
        assert.equal(edits[0].link_id, by['PL-1'].promotion.edges[0].link_id);
        assert(edits[0].comment.startsWith('Из предварительной связи PL-1 (анализ a631b49a)'), edits[0].comment);
        assert.equal(edits[0].reviewer_source, 'HUMAN');
        assert.deepEqual([reviews.length, reviews[0].status, reviews[0].region_id], [1, 'HUMAN_CONFIRMED', 'A-R007']);
        // Every dashed line stays within the band of the two panels, however far its ends are scrolled.
        await sleep(300);
        const outside = await page.evaluate(() => {
            const band = [...document.querySelectorAll('.sbm-scroll')].map(e => e.getBoundingClientRect());
            const top = Math.min(...band.map(r => r.top)) - 1, bottom = Math.max(...band.map(r => r.bottom)) + 1;
            return [...document.querySelectorAll('.sbm-prelink__line')].map(e => e.getBoundingClientRect())
                .filter(r => r.top < top || r.bottom > bottom).length;
        });
        assert.equal(outside, 0, 'dashed lines run outside the panels');
        await shot('r4-c1-confirmed');
    });
    await check('R5: the bridge sees exactly one anchor; the engine artifacts of the run are untouched', async () => {
        await ws().getByRole('button', {name: 'Проверка для нового анализа'}).click();
        await until(async () => (await workspaceText()).includes('якорей 1 · запретов 0'), 'bridge anchors');
        assert.deepEqual(fingerprint(runDir, rel => rel.startsWith('human_mapping')), runBefore);
    });
}

async function draftMode() {
    const runBefore = fingerprint(runDir);
    await openPair();
    await chooseBinding('Подготовка анализа · предварительные связи');
    await pl('connect').waitFor();
    const blocks = async side => ws().locator(`.sbm-panel--${side.toLowerCase()} [data-sbm-block]`).evaluateAll(
        els => els.filter(e => !e.className.includes('is-stamp')).map(e => e.dataset.sbmBlock));
    const pick = async (side, id) => {
        await ws().locator(`[data-sbm-side="${side}"][data-sbm-block="${id}"]`).first().click();
        await until(async () => /\bis-selected\b/.test(await ws().locator(`[data-sbm-side="${side}"][data-sbm-block="${id}"]`).first()
            .getAttribute('class')), `${side} ${id} selected`);
    };
    // A link may cross the saved sheet pairs: «+ стр.» brings a page into the lens.
    for (const [side, page_] of [['OLD', 17], ['NEW', 7]]) {
        await ws().getByLabel('Добавить страницу ' + side).fill(String(page_));
        await ws().getByLabel('Добавить страницу ' + side).dispatchEvent('change');
        await ws().locator(`[data-sbm-page="${side}-${page_}"]`).waitFor();
    }
    await page.waitForFunction(() => [...document.querySelectorAll('.sbm-page__img')].every(i => i.complete && i.naturalWidth > 0));
    const olds = await blocks('OLD'), news = await blocks('NEW');
    assert(olds.length >= 2 && news.length >= 2, `blocks OLD ${olds.length} NEW ${news.length}`);
    await check('D1: the preparation text says the AI does not see the links', async () => {
        assert((await workspaceText()).includes('ИИ их не видит; после анализа мы сравним их с результатом'));
        assert(norm(await ws().locator('.sbm-binding summary').innerText()).startsWith('Подготовка анализа · предварительные связи'));
    });
    const connect = async (o, n, kind, segments, node) => {
        for (const id of o) await pick('OLD', id);
        for (const id of n) await pick('NEW', id);
        assert.equal(norm(await ws().locator('.sbm-prelink-tools .sbm-hint-inline').innerText()).split(' (')[0],
            `Будет создана связь ${kind}`);
        const before = prelinkWrites().length;
        await pl('connect').click();
        await until(() => prelinkWrites().length === before + 1, 'POST /prelinks');
        await until(async () => (await pl('status').innerText()).includes('Сохранено · ред.'), 'saved');
        const line = ws().locator('.sbm-prelink').last();
        await until(async () => (await line.locator('.sbm-prelink__line').count()) === segments, `${kind}: ${segments} segments`);
        assert.equal(await line.locator('.sbm-prelink__node').count(), node ? 1 : 0);
        assert.notEqual(await line.locator('.sbm-prelink__line').first().evaluate(el => getComputedStyle(el).strokeDasharray), 'none');
    };
    await check('D2: 1→1, 1→N and N↔N are created through the UI; N↔N is spokes to one node', async () => {
        await connect([olds[0]], [news[0]], '1→1', 1, false);
        await connect([olds[1]], [news[0], news[1]], '1→N', 2, false);
        await connect([olds[0], olds[1]], [news[0], news[1]], 'N↔N', 4, true);
        assert.deepEqual(prelinkWrites().map(r => r.method), ['POST', 'POST', 'POST']);
        await shot('d2-created');
    });
    await check('D3: «Изменить состав» replaces the group (PUT); «Удалить» + «Вернуть» (DELETE, POST)', async () => {
        const view = await (await page.request.get(base + PRELINKS)).json();
        const first = view.prelinks.find(p => p.label === 'PL-1');
        await openFromList(first.prelink_id);
        await pl('edit').click();
        await pick('NEW', news[1]);
        await pl('save-edit').click();
        await until(() => prelinkWrites().some(r => r.method === 'PUT'), 'PUT');
        await until(async () => (await pl('status').innerText()).includes('Сохранено'), 'saved after PUT');
        await openFromList(first.prelink_id);
        await pl('delete').click();
        await until(async () => (await pl('status').innerText()).includes('Связь PL-1 удалена.'), 'deleted');
        await pl('undo').click();
        await until(() => prelinkWrites().filter(r => r.method === 'POST').length === 4, 'undo POST');
        let after;
        await until(async () => (after = await (await page.request.get(base + PRELINKS)).json()).prelinks.length === 3, 'restored');
        assert.deepEqual(after.prelinks.map(p => p.label).sort(), ['PL-2', 'PL-3', 'PL-4']);
        assert.equal(after.prelinks.find(p => p.label === 'PL-4').cardinality, '1:N');
    });
    await check('D4: the launch dialog tells what will be reconciled; nothing is launched', async () => {
        await page.getByRole('button', {name: '1. Загрузка документации', exact: true}).click();
        const pairRow = page.locator('.sc-pair-board__row', {hasText: 'АА_БЭ-03-ДС3-ИОС-2.1.pdf'});
        const menu = pairRow.locator('.pc-pair-menu summary');
        if (await menu.count()) await menu.click();
        await pairRow.getByRole('button', {name: /^(Перезапустить анализ пары|Запустить сравнение)$/}).first().click();
        const line = page.locator('[data-sbm-prelink="launch-line"]');
        await line.waitFor();
        assert.equal(norm(await line.innerText()), 'ⓘ Предварительные связи: 3. ИИ их не видит — после анализа мы сравним их с результатом.');
        await shot('d4-launch');
        await page.locator('.pause-modal .modal-close').click();
        await line.waitFor({state: 'detached'});
    });
    await check('D5: no Human Mapping write, no run touched, only the drafts file of the copy changed', async () => {
        assert.deepEqual(hmPosts(), []);
        assert(!requests.some(r => r.method === 'POST' && r.path.includes('/production/run')), 'no analysis launched');
        assert.deepEqual(fingerprint(runDir), runBefore);
        assert(fs.existsSync(path.join(state, 'comparison/sessions', SID, 'pairs', PID, 'prelink_drafts.json')));
    });
}

async function offMode() {
    await openPair();
    await check('O1: the feature off — no prelink request, no prelink element, the view of R2', async () => {
        assert.deepEqual(prelinkReads(), []);
        assert.equal(await ws().locator('.sbm-prelink, .sbm-prelink-tools, .sbm-prelink-summary, [data-sbm-prelink]').count(), 0);
        await ws().locator('.sbm-binding summary').click();
        const options = norm(await ws().locator('.sbm-binding .sbm-menu').innerText());
        assert(!options.includes('Подготовка анализа'), options);
        await ws().locator('.sbm-binding summary').click();
        assert(!(await workspaceText()).includes('ИИ их не видит'));
        assert.deepEqual(prelinkReads(), []);
        await shot('o1-off');
    });
}

(async () => {
    browser = await chromium.launch({headless: true, executablePath: process.env.CHROME_BIN || '/opt/google/chrome/chrome',
        args: ['--no-sandbox']});
    const context = await browser.newContext({viewport: {width: 1600, height: 1100}, serviceWorkers: 'block'});
    context.setDefaultTimeout(20000);
    await context.route('**/*', route => {
        const url = new URL(route.request().url());
        if (url.origin !== origin) { external.push(url.href); return route.abort(); }
        return route.continue();
    });
    context.on('request', r => { const u = new URL(r.url()); requests.push({method: r.method(), path: u.pathname + u.search}); });
    page = await context.newPage();
    page.on('pageerror', e => errors.push(e.message));
    try {
        if (mode === 'result') await resultMode();
        else if (mode === 'draft') await draftMode();
        else await offMode();
        await check('no model-facing request, no external request, no page error', async () => {
            assert(!requests.some(r => /\/production\/run\b/.test(r.path) && r.method === 'POST'));
            assert.deepEqual(external.filter(u => !/^https:\/\/fonts\.(googleapis|gstatic)\.com\//.test(u)), []);
            assert.deepEqual(errors, []);
        });
        save('PASS');
    } catch (error) {
        try { await shot('failure'); } catch (_) { /* best effort */ }
        save('FAIL', error);
        console.error(error);
        process.exitCode = 1;
    } finally {
        await browser.close();
    }
})();
