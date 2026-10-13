/* Actual shared template and Vue bindings, synthetic state, loopback only.
 * PLAYWRIGHT_MODULE=/path/to/playwright node --test tests/project_change_pipeline.browser.cjs
 */
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const {chromium} = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const frontend = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(frontend, 'index.html'), 'utf8');
const app = fs.readFileSync(path.join(frontend, 'static/js/app.js'), 'utf8');
const template = html.slice(html.indexOf('<div class="sc-steps-bar"'), html.indexOf('<sbm-link-notice'));
const bindings = app.slice(app.indexOf('const pcRunBanner ='), app.indexOf('// Последний этап прогона «Сведение дублей»'));
const ids = ['SOURCE_PREPARATION', 'MAPPING', 'MINING', 'UNMATCHED_REVIEW', 'DEDUPE', 'SOURCE_VERIFICATION', 'RESULT'];
const state = () => ({engine: 'projectchange_v3', run_id: 'synthetic-run', status: 'RUNNING',
    pipeline_current: 'MINING', current_stage: 'MINING',
    pipeline_stages: Object.fromEntries(ids.map((id, i) => [id, {status: i < 2 ? 'COMPLETED' : i === 2 ? 'RUNNING' : 'WAITING',
        ...(i === 2 ? {processed: 2, total: 12, unit: 'region'} : {})}]))});

test('pipeline appears only on tabs 2/3 and preserves progress across tabs/reload', async () => {
    let stored = state();
    const server = http.createServer((req, res) => {
        if (req.url === '/state') {res.setHeader('Content-Type', 'application/json'); return res.end(JSON.stringify(stored));}
        const files = {'/vue.js': 'static/js/vue.global.prod.js', '/view.js': 'static/js/project-change-view.js',
            '/styles.css': 'static/css/styles.css', '/pc.css': 'static/css/project-change-ui.css'};
        if (files[req.url]) {
            res.setHeader('Content-Type', req.url.endsWith('.css') ? 'text/css' : 'text/javascript');
            return res.end(fs.readFileSync(path.join(frontend, files[req.url])));
        }
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.end(`<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
            <link rel="stylesheet" href="/styles.css"><link rel="stylesheet" href="/pc.css">
            <style>body{display:block;padding:20px}#app{display:block}</style>
            <div id="app" class="pc-v1">${template}<p>{{ scTab === 'links' ? 'Сопоставление листов' : 'Изменения проекта' }}</p></div>
            <script src="/vue.js"></script><script src="/view.js"></script><script>
            const {createApp, ref, computed, watch} = Vue;
            createApp({setup() {
                const PC = ProjectChangeView, scActivePair = ref({id:'pair'}), scProductionState = ref(null);
                const pcReadOnlySources = ref(false), pcCatalogFocus = ref(null), scProductionClock = ref(Date.now());
                ${bindings}
                const scTab = ref('links');
                window.refreshState = async () => {scProductionState.value = await (await fetch('/state')).json();};
                window.pending = () => {scProductionState.value = {pipeline_pending:true, status:'RUNNING'};};
                window.refreshState();
                return {pcPipeline, pcPipelineExpanded, pcPipelineDetail, pcRunBanner, scTab, pcReadOnlySources,
                    pcUiEnabled:true, currentObjectId:'synthetic', scProductionError:'', scProductionCancelling:false, scProductionRunLoading:false,
                    scProductionRunActive:false, pcConsolidationStarting:false,
                    scCancelProductionRun(){}, scResumeProductionRun(){}, pcRetryConsolidation(){}, scOpenStageFolderDialog(){}};
            }}).mount('#app');</script></html>`);
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const browser = await chromium.launch({headless: true, executablePath: process.env.CHROME_BIN || '/opt/google/chrome/chrome',
        args: ['--no-sandbox']});
    try {
        const page = await browser.newPage({viewport: {width: 1684, height: 960}});
        const errors = [];
        page.on('pageerror', e => errors.push(e.message));
        const base = `http://127.0.0.1:${server.address().port}`;
        await page.route('**/*', route => new URL(route.request().url()).origin === base ? route.continue() : route.abort());
        await page.goto(base);
        const rail = page.locator('.pc-comparison-pipeline');
        await rail.waitFor();
        assert.equal(await rail.locator('button[data-stage]').count(), 7);
        assert.match(await rail.innerText(), /2 из 12 областей/);
        await rail.evaluate(el => {el.dataset.sameNode = 'yes';});
        await page.getByRole('button', {name: '3. Изменения проекта', exact: true}).click();
        assert.equal(await rail.getAttribute('data-same-node'), 'yes');
        await rail.locator('[data-stage="MINING"]').click();
        assert.match(await page.locator('#pc-comparison-pipeline-detail').innerText(), /Поиск изменений/);
        for (const label of ['1. Загрузка документации', '4. Отчёт']) {
            await page.getByRole('button', {name: label, exact: true}).click();
            assert.equal(await rail.count(), 0);
        }
        await page.getByRole('button', {name: '2. Сопоставление листов', exact: true}).click();
        await rail.waitFor();
        assert.match(await rail.innerText(), /2 из 12 областей/);
        assert.match(await page.locator('#pc-comparison-pipeline-detail').innerText(), /Поиск изменений/);
        stored.pipeline_stages.MINING.processed = 5;
        await page.evaluate(() => window.refreshState());
        assert.match(await rail.innerText(), /5 из 12 областей/);
        assert.match(await page.locator('#pc-comparison-pipeline-detail').innerText(), /5 из 12 областей/);
        await page.reload();
        await rail.waitFor();
        assert.match(await rail.innerText(), /5 из 12 областей/);
        if (process.env.PC_PIPELINE_SCREENSHOT) await page.screenshot({path: process.env.PC_PIPELINE_SCREENSHOT});
        await page.setViewportSize({width: 390, height: 844});
        assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
        assert(await rail.locator('.pipeline-detail').evaluate(el => el.scrollWidth > el.clientWidth));
        stored.status = 'REVIEW'; stored.pipeline_current = 'RESULT'; stored.consolidation_expected = true;
        stored.pipeline_stages = Object.fromEntries(ids.map(id => [id, {status:'COMPLETED'}]));
        stored.pipeline_stages.SOURCE_VERIFICATION = {status: 'PARTIAL', processed: 32, total: 69, unit: 'parameter',
            verification: {completed: 32, pending: 37, confirmed: 26, corrected: 1, conflicts: 0, unreadable: 5, failed: 0}};
        await page.evaluate(() => window.refreshState());
        assert.equal(await rail.locator('[data-stage="RESULT"]').getAttribute('data-status'), 'WAITING');
        const verification = rail.locator('[data-stage="SOURCE_VERIFICATION"]');
        assert.match(await verification.innerText(), /Не проверено: 37/);
        assert.match(await verification.innerText(), /Требуют разбора: 6/);
        await verification.click();
        assert.match(await page.locator('#pc-comparison-pipeline-detail').innerText(), /Подтверждено по источникам: 26/);
        assert.match(await page.locator('#pc-comparison-pipeline-detail').innerText(), /Не удалось прочитать источник: 5/);
        assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
        stored.consolidation = {status:'RUNNING', live:true, calls_done:1, calls_total:4};
        await page.evaluate(() => window.refreshState());
        assert.equal(await rail.locator('[data-stage="RESULT"]').getAttribute('data-status'), 'RUNNING');
        stored.consolidation = {status:'FAILED', message:'Тестовый сбой сведения'};
        await page.evaluate(() => window.refreshState());
        assert.equal(await rail.locator('[data-stage="RESULT"]').getAttribute('data-status'), 'FAILED');
        assert.equal(await rail.getByRole('button', {name:'Свести дубли', exact:true}).count(), 1);
        await page.evaluate(() => window.pending());
        assert.equal(await rail.locator('[data-stage="SOURCE_PREPARATION"]').getAttribute('data-status'), 'RUNNING');
        assert.deepEqual(errors, []);
    } finally {
        await browser.close();
        await new Promise(resolve => server.close(resolve));
    }
});
