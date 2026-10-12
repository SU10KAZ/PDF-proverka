/* Real comparison CSS/controls with synthetic documents; loopback only. */
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const {chromium} = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
const app = fs.readFileSync(path.join(root, 'static/js/app.js'), 'utf8');
const steps = html.slice(html.indexOf('<div class="sc-steps-bar"'), html.indexOf('<section v-if="pcUiEnabled && pcPipeline'));
const toolbarStart = html.indexOf('<div class="sc-viewer-toolbar">', html.indexOf('<div class="sc-viewer-shell">'));
const toolbar = html.slice(toolbarStart, html.indexOf('<div class="sc-viewer-layout"', toolbarStart));
const layout = app.slice(app.indexOf('let scScrollLayoutElement ='), app.indexOf('const projectAssembliesEnabled ='));

test('viewer scrolls up beneath the step bar and retains bounded inner scrolling', async () => {
    const server = http.createServer((req, res) => {
        const files = {'/vue.js': 'static/js/vue.global.prod.js', '/styles.css': 'static/css/styles.css',
            '/pc.css': 'static/css/project-change-ui.css', '/blocks.css': 'static/css/stage-block-mapping.css'};
        if (files[req.url]) {
            res.setHeader('Content-Type', req.url.endsWith('.css') ? 'text/css' : 'text/javascript');
            return res.end(fs.readFileSync(path.join(root, files[req.url])));
        }
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.end(`<!doctype html><html><meta charset="utf-8">
            <link rel="stylesheet" href="/styles.css"><link rel="stylesheet" href="/pc.css"><link rel="stylesheet" href="/blocks.css">
            <div id="app"><div class="main-area"><header class="header" style="height:56px">Объект</header>
            <div class="container pc-v1" :class="{'sc-container--fit':scTab==='links','sc-container--scroll':scTab==='links'}" :ref="scBindScrollLayout">
            ${steps}<section class="pc-comparison-pipeline" style="height:170px">Этапы анализа</section>
            <details class="pc-advanced"><summary>Дополнительно</summary></details>
            <div class="sc-shell sc-shell--viewer">
                <section class="sc-sheet-map"><div class="sc-sheet-map__head">Карта листов</div>
                    <div class="sc-sheet-map__list"><div style="height:120px">Сопоставленные листы</div></div></section>
                <div class="sc-viewer-shell">${toolbar}
                    <div v-if="scStage2Mode === 'blocks'" class="sbm-workspace"><div style="min-height:1400px">Смысловые блоки</div></div>
                    <div v-else class="sc-viewer-layout"><div class="sc-continuous-pane" style="flex:1"><div style="height:1800px">Листы PDF</div></div></div>
                </div>
            </div></div></div></div>
            <script src="/vue.js"></script><script>
            const {createApp,ref,onUnmounted,nextTick,watch} = Vue;
            createApp({setup(){
                const scTab=ref('upload'), scStage2Mode=ref('blocks');
                ${layout}
                return {scBindScrollLayout,scTab,scStage2Mode,scBlockStore:{segmentLabel:()=> 'Смысловые блоки'},pcBridgeActive:false,pcUiEnabled:true,
                    pcReadOnlySources:false,currentObjectId:'synthetic',scOpenStageFolderDialog(){},
                    scProductionEvidenceReturn:false,scZoomPercent:100,scBlockChip:()=>null,
                    scCurrentSheetMapRow:()=>null,scSetStage2Mode:mode=>scStage2Mode.value=mode,
                    scZoomBy(){},scZoomFit(){},scZoomActualSize(){}};
            }}).mount('#app');</script></html>`);
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const browser = await chromium.launch({headless:true, executablePath:'/opt/google/chrome/chrome', args:['--no-sandbox']});
    try {
        const page = await browser.newPage();
        page.setDefaultTimeout(5000);
        const errors=[];
        page.on('pageerror', error => errors.push(error.message));
        const base=`http://127.0.0.1:${server.address().port}`;
        await page.route('**/*', route => new URL(route.request().url()).origin === base ? route.continue() : route.abort());
        await page.goto(base);
        assert.deepEqual(errors,[]);
        await page.getByRole('button',{name:'2. Сопоставление листов',exact:true}).click();
        for (const viewport of [{width:1684,height:960}, {width:1280,height:720}]) {
            await page.setViewportSize(viewport);
            for (const mode of ['Смысловые блоки', 'Листы']) {
                await page.getByRole('button', {name:mode,exact:true}).click();
                await page.locator('.main-area').evaluate(el => el.scrollTop=0);
                await page.mouse.move(10,300);
                await page.mouse.wheel(0,2000);
                await page.waitForFunction(() => {
                    const r=s=>document.querySelector(s).getBoundingClientRect();
                    return Math.abs(r('.sc-viewer-toolbar').top-r('.sc-steps-bar').bottom)<3;
                }, null, {timeout:3000}).catch(async error => {
                    console.log(await page.evaluate(() => Object.fromEntries(
                        ['.main-area','.container','.header','.sc-steps-bar','.sc-shell--viewer','.sc-viewer-shell','.sc-viewer-toolbar']
                            .map(s=> {const el=document.querySelector(s);return [s,{rect:el.getBoundingClientRect().toJSON(),
                                scrollTop:el.scrollTop,scrollHeight:el.scrollHeight,style:el.getAttribute('style')}];}))));
                    throw error;
                });
                const sizes=await page.evaluate(() => {
                    const r=s=>document.querySelector(s).getBoundingClientRect();
                    return {pipeline:r('.pc-comparison-pipeline').bottom,steps:r('.sc-steps-bar').bottom,
                        header:r('.header').bottom,stepsTop:r('.sc-steps-bar').top,bottom:r('.sc-viewer-shell').bottom};
                });
                assert(sizes.pipeline<=sizes.steps);
                assert(Math.abs(sizes.header-sizes.stepsTop)<2);
                assert(sizes.bottom<=viewport.height);
                const inner=page.locator(mode==='Листы'?'.sc-continuous-pane':'.sbm-workspace');
                await inner.evaluate(el=>el.scrollTop=200);
                assert(await inner.evaluate(el=>el.scrollTop)>0);
            }
        }
        // Dynamic topbar size and expanded map must not leave the toolbar trapped below them.
        await page.locator('.header').evaluate(el=>el.style.height='88px');
        await page.locator('.sc-sheet-map__list > div').evaluate(el=>el.style.height='700px');
        await page.waitForTimeout(100);
        await page.locator('.main-area').evaluate(el=>el.scrollTop=el.scrollHeight);
        await page.waitForFunction(()=>Math.abs(document.querySelector('.sc-viewer-toolbar').getBoundingClientRect().top
            -document.querySelector('.sc-steps-bar').getBoundingClientRect().bottom)<3);
        assert.deepEqual(errors,[]);
    } finally {
        await browser.close();
        await new Promise(resolve=>server.close(resolve));
    }
});
