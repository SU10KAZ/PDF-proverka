/* Browser acceptance on the read-only local preview, not on production.
 * PLAYWRIGHT_MODULE=/tmp/project-change-ui-tools/node_modules/playwright node frontend/tests/project_change_ui.browser.cjs
 */
const {chromium} = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
const base = process.env.PC_PREVIEW_URL || 'http://127.0.0.1:8973';
if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(base)) throw new Error('Use a loopback-only fixture server');
const out = path.resolve(__dirname, '../../deliverables/project_change_ui_v1/screenshots');
fs.mkdirSync(out, {recursive: true});
(async () => {
    const browser = await chromium.launch({headless: true,
        ...(process.env.PC_CHROME ? {executablePath: process.env.PC_CHROME} : {}), args: ['--no-sandbox']});
    const page = await browser.newPage({viewport: {width: 1600, height: 1100}});
    const errors = [], mutations = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('console', message => {
        if (message.type() === 'error' && /SyntaxError|TypeError|ReferenceError/.test(message.text())) errors.push(message.text());
    });
    page.on('request', request => {
        if (['PUT', 'PATCH', 'DELETE'].includes(request.method())) mutations.push(request.url());
        if (request.method() === 'POST' && !/\/sessions$|\/pairs$/.test(request.url())) mutations.push(request.url());
    });
    let pass = 0;
    const check = async (label, run) => { await run(); pass++; console.log(`PASS ${label}`); };
    const tab = name => page.getByRole('button', {name, exact: true}).click();
    const card = () => page.locator('.pc-card').first();
    const settleImages = async () => {
        await page.locator('.pc-crop img').evaluateAll(images => Promise.all(images.map(img => {
            img.loading = 'eager';
            return img.complete ? Promise.resolve() : new Promise(resolve => { img.onload = resolve; img.onerror = resolve; });
        })));
    };
    const screenshot = async name => {
        await page.screenshot({path: path.join(out, name + '.png')});
    };
    try {
        await page.goto(base + '/?projectChangeUi=1#/stage-comparison');
        await page.locator('.sc-pair-board__row').first().waitFor();
        await check('four product tabs and compact upload list', async () => {
            assert.equal(await page.locator('.sc-steps-bar .project-tab').count(), 4);
            assert.equal(await page.locator('.sc-pair-board__row').count(), 3);
            assert.equal(await page.locator('.sc-production-pipeline').count(), 0);
            assert((await page.locator('.sc-pair-board__row').first().boundingBox()).y < 300);
            assert.equal(await page.getByRole('button', {name:'Запустить анализ проекта'}).isDisabled(), true);
        });
        await screenshot('page-1-upload');
        await tab('3. Изменения проекта');
        await card().waitFor();
        await settleImages();
        await check('research records start in REVIEW and have all routes', async () => {
            assert.equal(await page.locator('.pc-card').count(), 4);
            assert.equal(await page.locator('.pc-card[data-status="REVIEW"]').count(), 4);
            const text = await page.locator('.pc-workspace').innerText();
            for (const route of ['TEXT ✓', 'TABLE ✓', 'GRAPHIC ✓']) assert(text.includes(route));
            assert(!text.includes('Предварительный отчёт'));
            assert(!text.includes('OLD_SCOPE_NOT_ESTABLISHED'));
        });
        await check('dense layout has no horizontal document overflow', async () => {
            assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
            const old = await card().locator('.pc-states > div').nth(0).boundingBox();
            const newer = await card().locator('.pc-states > div').nth(1).boundingBox();
            assert.equal(old.y, newer.y); assert(newer.x > old.x);
        });
        await screenshot('page-3-changes');
        await check('multiple raster screenshots are loaded with context', async () => {
            assert.equal(await card().locator('.pc-crop img').count(), 6);
            assert(await card().locator('.pc-crop img').evaluateAll(images => images.every(img => img.naturalWidth > 0)));
            assert.equal(await card().locator('figcaption b').count(), 6);
        });
        await check('screenshot opens native dialog and Escape restores focus', async () => {
            await card().locator('.pc-crop').nth(1).click();
            assert.equal(await page.locator('dialog[open]').count(), 1);
            await screenshot('review-image-expanded');
            await page.keyboard.press('Escape');
            assert.equal(await page.locator('dialog[open]').count(), 0);
            assert(await card().locator('.pc-crop').nth(1).evaluate(el => el === document.activeElement));
        });
        await check('atomic details are collapsed and can be expanded', async () => {
            assert.equal(await card().locator('.pc-details').getAttribute('open'), null);
            await card().locator('.pc-details > summary').click();
            assert.equal(await card().locator('.pc-details table').isVisible(), true);
            assert.equal(await card().locator('.pc-technical').getAttribute('open'), null);
            await card().locator('.pc-details > summary').click();
        });
        await check('all five filters operate on the single list', async () => {
            const filters = page.locator('.pc-filters select');
            for (let n = 0; n < 5; n++) {
                const value = await filters.nth(n).locator('option').nth(1).getAttribute('value')
                    || await filters.nth(n).locator('option').nth(1).innerText();
                await filters.nth(n).selectOption(value);
                const matching = await page.locator('.pc-card').count();
                assert(matching <= 4);
                await filters.nth(n).selectOption('');
                assert.equal(await page.locator('.pc-card').count(), 4);
            }
            await filters.nth(4).selectOption('GRAPHIC');
            assert.equal(await page.locator('.pc-card').count(), 1);
            await filters.nth(4).selectOption('');
        });
        await check('PDF navigation opens exact pair, pages and both regions', async () => {
            // Select a GRAPHIC source; the opposite side uses its explicit TEXT evidence.
            await card().locator('figcaption .pc-link').nth(1).click();
            await page.locator('.sc-production-evidence-banner').waitFor();
            await page.waitForFunction(() => {
                const images = [...document.querySelectorAll('.sc-page-preview')];
                return images.length === 2 && images.every(img => img.complete && img.naturalWidth > 0);
            });
            const banner = await page.locator('.sc-production-evidence-banner').innerText();
            assert(banner.includes('стр. 22')); assert(banner.includes('стр. 16'));
            assert(banner.includes('область подсвечена'));
            assert.equal(await page.locator('.sc-production-evidence-overlay').count(), 2);
        });
        await check('sheet review filter skips confident saved links', async () => {
            await page.getByRole('button', {name: 'Требуют проверки (0)', exact: true}).click();
            assert.equal(await page.locator('.sc-sheet-map__row:visible').count(), 0);
            assert((await page.locator('.sc-sheet-map__empty').innerText()).includes('нет'));
            await page.locator('.pc-sheet-filter').getByRole('button', {name: 'Все', exact: true}).click();
            assert.equal(await page.locator('.sc-sheet-map__row:visible').count(), 1);
        });
        await check('viewer retains zoom and pair thumbnails', async () => {
            assert.equal(await page.getByRole('button', {name: 'Вписать', exact: true}).count(), 1);
            await page.getByRole('button', {name: 'Показать миниатюры', exact:true}).click();
            assert((await page.locator('.sc-thumbs__row').count()) > 0);
            await page.waitForFunction(() => [...document.querySelectorAll('.sc-thumbs img')]
                .every(img => img.complete && img.naturalWidth > 0));
        });
        await screenshot('page-2-sheets');
        await check('returns to the originating change', async () => {
            await page.getByRole('button', {name:'← Вернуться к той же строке', exact:true}).click();
            await card().waitFor();
            assert.equal(await page.locator('.pc-card').count(), 4);
        });
        await check('all four REVIEW actions update status', async () => {
            for (const [label, status] of [['Не изменение','REJECTED'], ['Не могу определить','UNDETERMINED'], ['Проблема','PROBLEM'], ['Подтвердить','CONFIRMED']]) {
                await card().getByRole('button', {name:label, exact:true}).click();
                assert.equal(await card().getAttribute('data-status'), status);
            }
        });
        await check('report excludes review and rejected records; exports are disabled', async () => {
            await tab('4. Отчёт');
            assert.equal(await page.locator('.pc-card').count(), 1);
            assert.equal(await card().getAttribute('data-status'), 'CONFIRMED');
            for (const format of ['Excel ↓','PDF ↓','HTML ↓']) assert(await page.getByRole('button', {name:format,exact:true}).isDisabled());
            const group = page.getByLabel('Группировка отчёта');
            for (const value of ['discipline','engineering_system','cipher']) await group.selectOption(value);
        });
        await screenshot('page-4-report');
        await check('demo decisions persist across reload without a backend write', async () => {
            await page.reload();
            // Existing workflow restores the active pair and opens the viewer.
            await page.locator('.sc-vector-pane-head').first().waitFor();
            await tab('3. Изменения проекта');
            assert.equal(await card().getAttribute('data-status'), 'CONFIRMED');
            assert.deepEqual(mutations, []);
        });
        await check('unbound OLD opens an empty side rather than unrelated PDF', async () => {
            const pump = page.locator('.pc-card').nth(1);
            assert((await pump.innerText()).includes('Привязка к OLD не установлена'));
            await pump.locator('figcaption .pc-link').first().click();
            await page.locator('.sc-production-evidence-banner').waitFor();
            assert((await page.locator('.sc-production-evidence-banner').innerText()).includes('Слева: доказательство не привязано'));
        });
        await check('reset returns report to empty state', async () => {
            await tab('3. Изменения проекта');
            await page.getByRole('button', {name:'Сбросить решения',exact:true}).click();
            await tab('4. Отчёт');
            assert.equal(await page.locator('.pc-card').count(), 0);
        });
        // Isolated component cases use an explicit synthetic test, never report truth.
        await page.evaluate(() => {
            const host = document.createElement('div'); host.id='pc-test-host'; host.className='pc-v1'; document.body.append(host);
            const v = ProjectChangeView.fromEnvelope({schema_version:'project-change-view/1',object_id:ProjectChangeView.OBJECT,
                origin:'RESEARCH',revision:'unit-test-only',items:[{id:'test-only',summary_ru:'Тест UI: конфликт источников',
                conflicts:[{values:[{source_type:'TEXT',value:'3'},{source_type:'TABLE',value:'2'},{source_type:'GRAPHIC',value:'2'}]}],
                evidence:[{id:'missing',source_type:'TEXT',side:'OLD',document:{},image_url:'/static/missing-test-image.png'}]}]}, ProjectChangeView.OBJECT);
            const testApp=Vue.createApp({template:'<project-change-list :changes="changes" :demo="true" :available="true"></project-change-list>',data:()=>({changes:v})});
            ProjectChangeUI.register(testApp); testApp.mount(host);
            window.pcTestApp=testApp;
        });
        await check('conflict shows per-source values and disables confirmation', async () => {
            const host = page.locator('#pc-test-host');
            await host.scrollIntoViewIfNeeded();
            assert((await host.innerText()).includes('Источники противоречат друг другу.'));
            for (const text of ['TEXT: 3', 'TABLE: 2', 'GRAPHIC: 2']) assert((await host.innerText()).includes(text));
            assert(await host.getByRole('button', {name:'Подтвердить',exact:true}).isDisabled());
        });
        await check('failed raster displays explicit fallback', async () => {
            await page.locator('#pc-test-host').getByText('Не удалось загрузить фрагмент.', {exact:true}).waitFor();
        });
        await page.evaluate(()=>{window.pcTestApp.unmount();document.querySelector('#pc-test-host').remove();});
        await check('feature flag off preserves existing workflow', async () => {
            await page.goto(base + '/#/stage-comparison');
            await page.getByRole('button',{name:'2. Связь блоков',exact:true}).waitFor();
            assert.equal(await page.locator('.pc-workspace').count(),0);
        });
        await check('no browser application errors', async () => assert.deepEqual(errors, []));
        console.log(`\n${pass} browser checks PASS`);
        fs.writeFileSync(path.join(out, '../browser-results.json'), JSON.stringify({passed:pass,errors,mutations},null,2));
    } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode=1; });
