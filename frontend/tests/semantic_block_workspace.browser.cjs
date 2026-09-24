/* Browser acceptance of stage 2 «Смысловые блоки» (view only) against the stand, never :8081.
   Start a FRESH stand first (the test writes Human Mapping rows into the stand's copy); the source root is
   only read, the run a631b49a of p290a06df79 must have no Human Mapping history yet:
     python scripts/stage_block_mapping_smoke_server.py --copy-from comparison \
         --pairs e6fc8a2725eb4a67/p290a06df79 e6fc8a2725eb4a67/p11ad4a09d9
   p11ad4a09d9 brings the sealed snapshot pcv3snap_5157… with its legacy history human_mapping/4f3e5916/.
   Status-only states (K0, K-PDF, K-BLOCKS, M1) patch the real status response (page.route).
     NODE_PATH=<dir with playwright> node frontend/tests/semantic_block_workspace.browser.cjs */
const {chromium} = require('playwright');
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');

const base = process.env.SMOKE_BASE || 'http://127.0.0.1:8992';
const origin = new URL(base).origin;
assert(['127.0.0.1', 'localhost'].includes(new URL(base).hostname) && new URL(base).port !== '8081',
    'The block mapping acceptance runs only against the loopback stand');
const out = process.env.SMOKE_OUTPUT || '/home/coder/auditmanager/build-tmp/sbm-browser';
fs.mkdirSync(out, {recursive: true});
const SID = 'e6fc8a2725eb4a67', PID = 'p290a06df79', RUN = 'a631b49aaaac4db0af66a495c155c629', OID = '4f3e5916';
const BM = `/api/stage-comparison/sessions/${SID}/pairs/${PID}/block-mapping`;
const HM = `/api/human-mapping/objects/${OID}/comparisons/${PID}`;
const SCOPE = `session_id=${SID}&run_id=${RUN}`;
const B = {r16old: 'blk_d2ebac1ee4424ad596bcbe459b211e88', r16new: 'blk_55bd55313fe8409cbb6cfd34b9348536',
    m2new: 'blk_ab9a0461cbe44d2395af7af13d3d9dd3', p1old: 'blk_4adde776708344bd8b14c465084fdde4',
    p1new: 'blk_0e3a394a3ef74e54952160f1bea58dad'};

const checks = [], requests = [], errors = [], external = [];
let browser, context, page;
const save = (status, error) => fs.writeFileSync(path.join(out, 'browser-results.json'), JSON.stringify(
    {status, error: error ? String(error.stack || error) : null, base, checks, errors, external,
        requests: requests.map(r => r.method + ' ' + r.path)}, null, 2));
async function check(name, fn) { await fn(); checks.push({name, status: 'PASS'}); console.log('PASS ' + name); save('RUNNING'); }
const shot = name => page.screenshot({path: path.join(out, name + '.png'), animations: 'disabled'});
const since = mark => requests.slice(mark);
const count = (list, re) => list.filter(r => re.test(r.path)).length;
const norm = text => text.replace(/\s+/g, ' ');                   // innerText keeps nbsp/newlines between inline links
const workspaceText = async () => norm(await page.locator('.sbm-workspace').innerText());

async function openCatalogPair(label) {
    await page.getByRole('button', {name: '1. Загрузка документации', exact: true}).click();
    await page.locator('tr', {hasText: label}).getByRole('button', {name: 'Открыть', exact: true}).first().click();
    // pcOpenCatalogEntry ends on step 3; switch to step 2 only after it has finished.
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
async function hmPost(kind, body) {
    const response = await page.request.post(`${base}${HM}/${kind}?${SCOPE}`, {data: body});
    assert.equal(response.status(), 200, `${kind}: ${await response.text()}`);
}
async function withStatus(patch, fn) {
    await page.route('**' + BM + '/status', async route => {
        const response = await route.fetch();
        const body = await response.json();
        patch(body.runs.find(r => r.run_id === RUN));
        await route.fulfill({response, json: body});
    });
    try { await fn(); } finally { await page.unroute('**' + BM + '/status'); }
}
async function reloadToRow(prefix) {
    await page.reload();
    await page.locator('.sc-sheet-map__semantic').first().waitFor();
    await openChip(prefix);
}

(async () => {
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

    await page.goto(base + '/#/stage-comparison');
    await page.locator('tr', {hasText: 'ИОС2.1'}).first().waitFor();

    await check('P1: no block mapping request before a pair is chosen', async () => {
        assert.equal(count(requests, /\/block-mapping\/|\/api\/human-mapping\//), 0);
    });

    const opened = requests.length;
    await openCatalogPair('ИОС2.1');
    await page.locator('.sc-sheet-map__semantic').first().waitFor();
    await page.waitForFunction(() => /Смысловые связи · прогон a631b49a/.test(document.body.innerText));

    await check('P1: after the pair — one status, one index, one reviews, one block-links; nothing lazy yet', async () => {
        const list = since(opened);
        assert.equal(count(list, /\/block-mapping\/status$/), 1);
        assert.equal(count(list, /\/region-index$/), 1);
        assert.equal(count(list, /\/api\/human-mapping\/.*\/reviews\?/), 1);
        assert.equal(count(list, /\/api\/human-mapping\/.*\/block-links\?/), 1);
        for (const lazy of [/\/page-blocks\?/, /\/blocks\/(OLD|NEW)\//, /\/bridge-check$/, /\/ui-data\?/, /\/api\/human-mapping\/.*\/assets\//])
            assert.equal(count(list, lazy), 0, String(lazy));
    });

    await check('Pages mode by default: summary, zero-free chips, untouched regions row', async () => {
        const text = norm(await page.locator('.sc-shell--viewer').innerText());
        assert(text.includes('Смысловые связи · прогон a631b49a: закреплено 0 · запрещено 0 · требуют внимания 0'));
        assert(/◇ Смысловые регионы вне сопоставленных пар листов: 3 ?— ?A-R003 ?· ?A-R005 ?· ?A-R015/.test(text), text.slice(0, 600));
        const chips = await page.locator('.sc-sheet-map__semantic').allTextContents();
        assert(chips.map(c => c.trim()).includes('◇6'));
        assert(chips.every(c => !/[✓⊘⚠]0/.test(c)));
        assert.equal(await page.locator('.sc-viewer-layout').isVisible(), true);
        await shot('01-pages-mode');
    });

    const blocksMark = requests.length;
    await openChip('◇6');
    await rastersLoaded();
    await check('Row OLD 20 ⇄ NEW 10: six regions on shelves, H and L texts, rasters once with the viewer signature', async () => {
        const text = await workspaceText();
        assert(text.includes('Пара листов: OLD лист 5 (стр. 20) ↔ NEW лист 5 (стр. 10) · регионов: 6'));
        assert(text.includes('Решения по блокам не меняют полученный результат анализа (64 изменения).'));
        assert(text.includes('Редактирование смысловых связей в этом разделе ещё не включено. Для правки откройте Human Mapping ↗'));
        assert.equal(await page.locator('.sbm-segment button.is-active').innerText(), 'Смысловые блоки ◇6');
        const previews = since(blocksMark).filter(r => /\/page-preview\?/.test(r.path)).map(r => r.path);
        for (const [side, pageNo] of [['left', 20], ['right', 10]]) {
            const hits = previews.filter(p => p.includes(`side=${side}&page=${pageNo}&width=1400&`));
            assert.equal(hits.length, 1, JSON.stringify(previews));
            assert(/&v=\d+%3A\d+$/.test(hits[0]), hits[0]);            // mtime_ns:size, the viewer's cache key
        }
        assert.equal(count(since(blocksMark), /\/api\/human-mapping\/.*\/assets\//), 0);
        await shot('02-row-20-10');
    });

    await check('«Весь регион» A-R016: OLD 20, 51, 52 · NEW 5, 10, 26 with in/out of pair tags', async () => {
        await page.locator('.sbm-workspace').getByRole('button', {name: 'Весь регион', exact: true}).click();
        await page.waitForFunction(() => document.querySelectorAll('.sbm-page').length === 6);
        const labels = (await page.locator('.sbm-page__label').allTextContents()).map(t => t.replace(/\s+/g, ' ').trim());
        for (const p of ['стр. 20', 'стр. 51', 'стр. 52', 'стр. 5 ', 'стр. 10', 'стр. 26'])
            assert(labels.some(l => (l + ' ').includes(p)), p + ' in ' + JSON.stringify(labels));
        assert.equal(labels.filter(l => l.includes('в паре')).length, 2);
        await shot('03-whole-region');
    });

    // History written through the classic Human Mapping API into the stand's copy.
    await hmPost('block-links', {event_type: 'ADD_BLOCK_LINK', region_id: 'A-R016', link_id: 'sbm-r16',
        old_block_id: B.r16old, new_block_id: B.r16new, comment: 'стенд'});
    await hmPost('reviews', {status: 'HUMAN_CONFIRMED', region_id: 'A-R016', old_block_ids: [B.r16old],
        new_block_ids: [B.r16new], comment: 'стенд'});
    await hmPost('block-links', {event_type: 'ADD_BLOCK_LINK', region_id: 'A-R010', link_id: 'sbm-m2-a',
        old_block_id: B.r16old, new_block_id: B.m2new, comment: 'стенд'});
    await hmPost('reviews', {status: 'HUMAN_CONFIRMED', region_id: 'A-R010', old_block_ids: [B.r16old],
        new_block_ids: [B.m2new], comment: 'стенд'});
    await hmPost('block-links', {event_type: 'ADD_BLOCK_LINK', region_id: 'A-R014', link_id: 'sbm-m2-b',
        old_block_id: B.r16old, new_block_id: B.m2new, comment: 'стенд'});
    await hmPost('reviews', {status: 'HUMAN_REJECTED', region_id: 'A-R014', old_block_ids: [B.r16old],
        new_block_ids: [B.m2new], comment: 'стенд'});
    await hmPost('block-links', {event_type: 'ADD_BLOCK_LINK', region_id: 'A-R001', link_id: 'sbm-p1',
        old_block_id: B.p1old, new_block_id: B.p1new, comment: 'стенд'});

    await reloadToRow('◇6');
    await rastersLoaded();
    await check('Anchor line ✓ between block centres; M2 conflict named with both regions; the bridge check agrees', async () => {
        await page.locator('.sbm-edge__glyph', {hasText: '✓'}).first().waitFor();
        const text = await workspaceText();
        assert(text.includes('Одна и та же связь закреплена в A-R010 и запрещена в A-R014 — снимок для нового анализа не соберётся.'));
        const before = requests.length;
        await page.locator('.sbm-workspace').getByRole('button', {name: 'Проверка для нового анализа', exact: true}).click();
        await page.waitForFunction(() => document.querySelector('.sbm-workspace').innerText.replace(/\s+/g, ' ')
            .includes('Не собирается: одна и та же связь закреплена в одном регионе и запрещена в другом.'));
        assert.equal(count(since(before), /\/bridge-check$/), 1);
        const summary = norm(await page.locator('.sc-sheet-map__head').innerText());
        assert(/Смысловые связи · прогон a631b49a: закреплено [1-9]\d* · запрещено [1-9]\d* · требуют внимания \d+/.test(summary), summary);
        await shot('04-anchor-and-conflict');
    });

    await check('P: one-sided sheet — one panel, a port for the NEW end, «Весь регион» shows both sides', async () => {
        await page.getByRole('button', {name: 'Листы', exact: true}).click();
        await openChip('◇1');
        const text = await workspaceText();
        assert(/У листа .+ нет пары\. Показана одна сторона; чтобы соединить блоки, включите «Весь регион»\./.test(text), text);
        assert.equal(await page.locator('.sbm-panel:visible').count(), 1);
        await page.locator('.sbm-port').first().waitFor();
        await shot('05-state-p');
        await page.locator('.sbm-region', {hasText: 'A-R001'}).first().click();     // «Весь регион» needs a focused region
        await page.locator('.sbm-workspace').getByRole('button', {name: 'Весь регион', exact: true}).click();
        await page.waitForFunction(() => [...document.querySelectorAll('.sbm-panel')].filter(p => p.offsetParent).length === 2);
        await page.locator('.sbm-edge__line').first().waitFor();
    });

    const patched = [
        ['K0', r => Object.assign(r, {source_stale: true, blocks_content_match: true}),
            'Файлы распознавания перезаписаны без изменений — работа с блоками доступна.'],
        ['K-PDF', r => Object.assign(r, {pdf_match: false, write_block_reason: 'SOURCE_PDF_CHANGED',
            write_block_reasons: ['SOURCE_PDF_CHANGED', 'WRITES_DISABLED']}),
            'Документ пары изменился после анализа a631b49a. Решения нельзя будет использовать — перезапустите анализ.'],
        ['K-BLOCKS', r => Object.assign(r, {source_stale: true, blocks_content_match: false, write_block_reason: 'SOURCE_BLOCKS_CHANGED',
            write_block_reasons: ['SOURCE_BLOCKS_CHANGED', 'WRITES_DISABLED']}),
            'Распознавание пары изменилось после анализа a631b49a. Новые решения могли бы указывать на другие блоки, поэтому запись закрыта.'],
        ['M1', r => Object.assign(r, {history: {...r.history, poisoned: true, poison_codes: ['BLOCK_OUTSIDE_REGION']},
            write_block_reason: 'HISTORY_POISONED', write_block_reasons: ['HISTORY_POISONED', 'WRITES_DISABLED']}),
            'История прогона содержит событий, которые проверка для нового анализа отвергнет: '],
    ];
    for (const [name, patch, expected] of patched) {
        await check(`${name}: the first write_block_reason is the one shown`, async () => {
            await withStatus(patch, async () => {
                await reloadToRow('◇6');
                await page.waitForFunction(t => document.querySelector('.sbm-workspace').innerText.replace(/\s+/g, ' ').includes(t), expected);
                if (name === 'K-PDF') {
                    // C9: the live PDF is not the run's source, so the run's own raster is shown.
                    await page.waitForFunction(() => [...document.querySelectorAll('.sbm-page__img')]
                        .some(i => i.getAttribute('src').includes('/assets/old/p020/full_page.png?session_id=')));
                }
                if (name !== 'K0') assert(!(await workspaceText()).includes('Редактирование смысловых связей в этом разделе ещё не включено.'));
            });
        });
    }

    await check('G-SNAP: the sealed snapshot opened from the catalog, legacy journal as in standalone HM', async () => {
        const mark = requests.length;
        await openCatalogPair('АР1');
        await page.getByRole('button', {name: /^Смысловые блоки/}).click();
        await page.locator('.sbm-workspace').waitFor();
        await page.waitForFunction(() => document.querySelector('.sbm-workspace').innerText.replace(/\s+/g, ' ').includes('Снимок pcv3snap_5157… · только просмотр'));
        const text = await workspaceText();
        assert(text.includes('Решения этого снимка хранятся отдельно от результатов анализа и не могут стать якорями нового анализа.'));
        const hm = since(mark).filter(r => /\/api\/human-mapping\//.test(r.path)).map(r => r.path);
        assert(hm.length && hm.every(p => p.includes('result_id=pcv3snap_5157938d548fb1df6fbcf87c200c1347')), JSON.stringify(hm));
        await page.locator('.sbm-workspace summary', {hasText: 'Все регионы'}).click();          // <details> navigator
        await page.locator('.sbm-nav__item', {hasText: 'A-R001'}).first().click();
        await page.locator('.sbm-workspace summary', {hasText: 'Журнал региона (2)'}).click();
        const journal = page.locator('.sbm-journal li');
        await journal.nth(1).waitFor();
        assert.deepEqual(await journal.evaluateAll(els => els.map(e => e.classList.contains('is-superseded'))), [true, false]);
        assert((await journal.nth(0).innerText()).includes('заменено'));
        await shot('07-g-snap');
    });

    await check('E: catalog «Смысловые блоки в шаге 2» opens stage 2 in the blocks mode with a focused region (C13)', async () => {
        await page.getByRole('button', {name: '1. Загрузка документации', exact: true}).click();
        await page.locator('tr', {hasText: 'ИОС2.1'}).getByRole('button', {name: 'Смысловые блоки в шаге 2', exact: true}).click();
        await page.locator('.sbm-workspace').waitFor();
        await page.waitForFunction(() => document.querySelector('.sbm-segment button.is-active')?.textContent.startsWith('Смысловые блоки'));
        await page.locator('.sbm-region.is-focus').first().waitFor();
        await shot('08-catalog-blocks');
    });

    await check('E: deep link opens exactly its row and region; an unknown run is shown as such, not replaced', async () => {
        const link = `#/stage-comparison?object=${OID}&session=${SID}&pair=${PID}&view=blocks&run=${RUN}&lp=20&rp=10&region=A-R010`;
        await page.goto(base + '/' + link);
        await page.waitForFunction(() => (document.querySelector('.sbm-workspace')?.innerText || '').replace(/\s+/g, ' ')
            .includes('Пара листов: OLD лист 5 (стр. 20) ↔ NEW лист 5 (стр. 10)'));
        await page.locator('.sbm-region.is-focus', {hasText: 'A-R010'}).first().waitFor();
        const ghost = 'f'.repeat(32);
        await page.goto(base + '/' + link.replace(RUN, ghost));
        await page.waitForFunction(() => /Результат ffffffff недоступен или изменён вне системы\./
            .test((document.querySelector('.sbm-workspace')?.innerText || '').replace(/\s+/g, ' ')));
        // The plaque lists the pair's results to choose from; the binding itself stays on the run of the link.
        const bound = await page.evaluate(() => document.querySelector('#app').__vue_app__._container._vnode.component
            .setupState.scBlockStore.state.binding);
        assert.deepEqual(bound, {kind: 'LIVE', runId: ghost}, 'must not silently switch to the current run');
        await page.goto(base + `/#/stage-comparison?object=${OID}&session=${SID}&pair=${PID}&view=blocks&run=run1`);
        await page.locator('.sbm-link-notice', {hasText: 'Ссылка повреждена: параметры не распознаны.'}).waitFor();
    });

    await check('Narrow screens: one panel with OLD/NEW tabs at 700 px, a list at 480 px, no sideways scroll', async () => {
        await openCatalogPair('ИОС2.1');
        await openChip('◇6');
        for (const [width, layout] of [[700, 'single'], [480, 'list']]) {
            await page.setViewportSize({width, height: 1000});
            await page.waitForFunction(l => document.querySelector('.sbm-workspace').classList.contains('sbm-layout--' + l), layout);
            const overflow = await page.locator('.sbm-workspace').evaluate(el => el.scrollWidth - el.clientWidth);
            assert(overflow <= 1, `${width}px overflow ${overflow}`);
            if (layout === 'single') assert.equal(await page.locator('.sbm-tabs [role=tab]').count(), 2);
            await shot('06-width-' + width);
        }
        await page.setViewportSize({width: 1600, height: 1100});
    });

    await check('No external request, no page error', async () => {
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
