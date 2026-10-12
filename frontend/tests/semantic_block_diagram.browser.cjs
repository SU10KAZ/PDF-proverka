// Real Vue component and CSS, synthetic documents only; no external requests or writes.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const {chromium} = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const root = path.resolve(__dirname, '..');

test('whole sheets with multiple block frames, attached links, lazy pages and safe remount', async () => {
    const requests = [];
    const server = http.createServer((req, res) => {
        requests.push({url: req.url, method: req.method});
        const assets = {'/vue.js': 'js/vue.global.prod.js', '/core.js': 'js/human-mapping-core.js',
            '/blocks.js': 'js/stage-block-mapping.js', '/styles.css': 'css/styles.css',
            '/blocks.css': 'css/stage-block-mapping.css'};
        if (assets[req.url]) {
            res.setHeader('Content-Type', req.url.endsWith('.css') ? 'text/css' : 'text/javascript');
            return res.end(fs.readFileSync(path.join(root, 'static', assets[req.url])));
        }
        if (req.url.startsWith('/raster/')) {
            res.setHeader('Content-Type', 'image/svg+xml');
            return res.end('<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="1400"><rect width="1000" height="1400" fill="white"/><rect x="40" y="40" width="920" height="1320" fill="none" stroke="#333"/><polygon points="100,140 900,140 900,350 400,350 400,560 100,560" fill="#eff8f7"/><rect x="100" y="770" width="600" height="350" fill="#faf6ed"/><text x="150" y="300" font-size="45">Первый блок на листе</text><text x="150" y="900" font-size="40">Второй блок</text><text x="620" y="1300" font-size="28">Штамп документа</text></svg>');
        }
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.end(`<!doctype html><html><meta charset="utf-8"><link rel="stylesheet" href="/styles.css"><link rel="stylesheet" href="/blocks.css">
            <div id="app" style="height:100vh;display:flex;flex-direction:column"><semantic-block-comparison v-if="mounted" :store="store"/></div>
            <script src="/vue.js"></script><script src="/core.js"></script><script src="/blocks.js"></script><script>
            const {reactive, ref, markRaw} = Vue, api = StageBlockMapping;
            const S = reactive({pages:{OLD:{},NEW:{}},geometry:{},loading:false,version:0});
            const makeRegion = (i) => api.fromIndexRegion({id:'R'+i, domain:'Смысловой блок '+i,
                members:{OLD:[{id:'o'+i,page:Math.ceil(i/2),type:'TEXT'}],NEW:[{id:'n'+i,page:Math.ceil(i/2)+20,type:'TEXT'}]}});
            let model = api.buildModel({regions:Array.from({length:40}, (_,i)=>makeRegion(i+1))});
            model.edges.set('o1|n2', {key:'o1|n2', old_block_id:'o1', new_block_id:'n2', oldPage:1, newPage:21});
            model.edges.set('o1|n40', {key:'o1|n40', old_block_id:'o1', new_block_id:'n40', oldPage:1, newPage:40});
            const loaded = new Set(); window.pageReads=[];
            const store = markRaw({state:S, model(){ void S.version; return model; },
                ensureDiagramPage(side,page){
                    const key=side+page; if(loaded.has(key))return; loaded.add(key); window.pageReads.push(key);
                    const n=side==='OLD'?page:page-20, prefix=side==='OLD'?'o':'n';
                    S.pages[side][page]={status:'ok', geometry:{width_px:1000,height_px:1400},
                        blocks:[{block_id:prefix+(n*2-1),bbox:[.1,.1,.9,.4],shape_type:'polygon',
                            polygon_points:[[.1,.1],[.9,.1],[.9,.25],[.4,.25],[.4,.4],[.1,.4]]},
                            {block_id:prefix+(n*2),bbox:[.1,.55,.7,.8]}]};
                },
                pageView(side,page){return S.pages[side][page] || {status:'loading',blocks:[]};},
                rasterUrl(side,page){return '/raster/'+side+'/'+page;}, retry(){S.statusError=null;}
            });
            const mounted=ref(true); window.fixture={S,mounted,empty(){model=null;S.version++;}};
            const app=Vue.createApp({setup:()=>({store,mounted})}); api.register(app); app.mount('#app');
            </script></html>`);
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const browser = await chromium.launch({headless:true, executablePath:'/opt/google/chrome/chrome', args:['--no-sandbox']});
    try {
        const page = await browser.newPage({viewport:{width:1440,height:900}});
        page.setDefaultTimeout(5000);
        const errors = []; page.on('pageerror', e=>errors.push(e.message));
        const base = 'http://127.0.0.1:'+server.address().port;
        await page.route('**/*', route => new URL(route.request().url()).origin === base ? route.continue() : route.abort());
        await page.goto(base);
        await page.locator('[data-link="o1|n40"]').waitFor({state:'attached'});
        await page.waitForFunction(()=>document.querySelector('.sbm-simple__image')?.naturalWidth>0);
        assert.equal(await page.locator('.sbm-simple__sheet').count(),40);
        const firstSheet=page.locator('.sbm-simple__sheet[data-side="OLD"][data-page="1"]');
        assert.equal(await firstSheet.locator('.sbm-simple__block').count(),2);
        assert.equal(await firstSheet.locator('img').count(),1);
        assert.equal(await page.locator('.sbm-simple__card, .sbm-simple__crop, .sbm-bar, .sbm-shelves, .sbm-decision, select, input').count(),0);
        assert(await page.evaluate(()=>window.pageReads.length<20), 'off-screen pages stay unloaded');
        assert(await page.evaluate(()=>window.pageReads.includes('NEW40')), 'distant link endpoint has coordinates');
        assert.equal(await page.locator('.sbm-simple__sheet[data-side="NEW"][data-page="40"] img').count(),0, 'distant raster stays unloaded');
        assert.equal(await page.locator('.sbm-simple__lines path').first().evaluate(el=>getComputedStyle(el).strokeDasharray),'7px, 5px');
        const geometry = () => page.evaluate(()=>{
            const canvas=document.querySelector('.sbm-simple__canvas').getBoundingClientRect();
            const old=document.querySelector('[data-block="o1"]').getBoundingClientRect();
            const next=document.querySelector('[data-block="n1"]').getBoundingClientRect();
            const line=document.querySelector('[data-link="o1|n1"]');
            const sheet=document.querySelector('[data-block="o1"]').closest('.sbm-simple__page').getBoundingClientRect();
            const start=line.getPointAtLength(0),end=line.getPointAtLength(line.getTotalLength());
            return {dx:Math.abs(start.x-(old.right-canvas.left)),startY:Math.abs(start.y-(sheet.top+sheet.height*.175-canvas.top)),
                dy:Math.abs(end.y-(next.top+next.height/2-canvas.top)),
                left:old.left,right:next.left, frameX:(old.left-sheet.left)/sheet.width,
                frameY:(old.top-sheet.top)/sheet.height, frameWidth:old.width/sheet.width};
        });
        let g=await geometry(); assert(g.dx<1 && g.dy<1 && g.startY<1 && g.left<g.right);
        assert(Math.abs(g.frameX-.1)<.002 && Math.abs(g.frameY-.1)<.002 && Math.abs(g.frameWidth-.8)<.002, 'frame follows normalized page coordinates');
        assert.equal(await page.locator('[data-block="o1"]').evaluate(el=>el.tagName),'polygon');
        assert.equal(await page.locator('[data-block="o1"]').getAttribute('points'), '0.1,0.1 0.9,0.1 0.9,0.25 0.4,0.25 0.4,0.4 0.1,0.4');
        // The cut-out part of this L-shaped block must not intercept clicks.
        assert(await page.locator('.sbm-simple__page').first().evaluate(el=>{
            const r=el.getBoundingClientRect();
            return !document.elementFromPoint(r.left+r.width*.7,r.top+r.height*.35)?.closest('[data-block]');
        }));
        await page.locator('[data-block="o1"]').focus();
        await page.keyboard.press('Enter');
        assert.equal(await page.locator('.sbm-simple__lines path.is-active').count(),3);
        assert.equal(await page.locator('.sbm-simple__block.is-related').count(),4);
        const hide = page.getByRole('button', {name:'Скрыть все блоки', exact:true});
        await hide.waitFor();
        const menu = await hide.boundingBox(), selected = await page.locator('[data-block="o1"]').boundingBox();
        assert(Math.abs(menu.y-selected.y)<40, 'action appears next to the selected block');
        const allBlocks = await page.locator('.sbm-simple__block:visible').count();
        await hide.click();
        assert.equal(await page.locator('.sbm-simple__block:visible').count(),4);
        assert.equal(await page.locator('.sbm-simple__sheet:visible').count(),3);
        assert.equal(await page.locator('.sbm-simple__sheet[data-side="OLD"]:visible').count(),1);
        assert.equal(await page.locator('.sbm-simple__sheet[data-side="NEW"][data-page="40"]').isVisible(),true);
        assert.equal(await page.locator('.sbm-simple__lines path').count(),3);
        assert.equal(await page.locator('[data-block="o2"]').isVisible(),false);
        await page.getByRole('button', {name:'Показать все блоки', exact:true}).click();
        assert.equal(await page.locator('.sbm-simple__sheet:visible').count(),40);
        assert(await page.locator('.sbm-simple__block:visible').count() >= allBlocks);
        await hide.click();
        await page.keyboard.press('Escape');
        assert.equal(await page.locator('.sbm-simple__sheet:visible').count(),40);
        assert(await page.locator('.sbm-simple__block:visible').count() >= allBlocks);
        assert.equal(await page.locator('.sbm-simple__selection-tool').count(),0);
        await page.locator('[data-block="o1"]').focus();
        await page.keyboard.press('Enter');
        await page.screenshot({path:'/tmp/semantic-block-polygons-desktop.png'});
        await page.locator('.sbm-simple__sheet[data-side="OLD"][data-page="10"]').scrollIntoViewIfNeeded();
        await page.waitForFunction(()=>window.pageReads.includes('OLD10'));
        g=await geometry(); assert(g.dx<1 && g.dy<1, 'links stay attached while scrolling');
        await page.setViewportSize({width:600,height:800});
        await page.waitForFunction(()=>Number(document.querySelector('.sbm-simple__lines').getAttribute('width'))<=600);
        g=await geometry(); assert(g.dx<1 && g.dy<1 && g.left<g.right);
        await page.evaluate(()=>window.fixture.mounted.value=false);
        await page.evaluate(()=>window.fixture.mounted.value=true);
        await page.locator('[data-link="o1|n1"]').waitFor({state:'attached'});
        await page.evaluate(()=>window.fixture.S.statusError={code:'test'});
        await page.getByRole('alert').waitFor();
        assert.equal(await page.locator('.sbm-simple__sheet').count(),0);
        await page.getByRole('button',{name:'Повторить'}).click();
        await page.waitForFunction(()=>document.querySelectorAll('.sbm-simple__sheet').length===40);
        await page.evaluate(()=>window.fixture.empty());
        await page.getByRole('status').waitFor();
        await page.waitForFunction(()=>document.querySelectorAll('.sbm-simple__lines path').length===0);
        assert.deepEqual(errors,[]);
        assert(requests.every(r=>r.method==='GET'));
    } finally { await browser.close(); await new Promise(resolve=>server.close(resolve)); }
});
