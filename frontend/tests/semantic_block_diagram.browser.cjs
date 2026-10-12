// Real Vue component and CSS, synthetic documents only; no external requests or writes.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const {chromium} = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const root = path.resolve(__dirname, '..');

test('two project columns, accurate dashed links, lazy crops and safe remount', async () => {
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
            return res.end('<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="1400"><rect width="1000" height="1400" fill="white"/><rect x="100" y="140" width="800" height="840" fill="#eff8f7" stroke="#008f85"/><text x="150" y="300" font-size="50">Документ · Блок</text></svg>');
        }
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.end(`<!doctype html><html><meta charset="utf-8"><link rel="stylesheet" href="/styles.css"><link rel="stylesheet" href="/blocks.css">
            <div id="app" style="height:100vh;display:flex;flex-direction:column"><semantic-block-comparison v-if="mounted" :store="store"/></div>
            <script src="/vue.js"></script><script src="/core.js"></script><script src="/blocks.js"></script><script>
            const {reactive, ref, markRaw} = Vue, api = StageBlockMapping;
            const S = reactive({pages:{OLD:{},NEW:{}},geometry:{},loading:false,version:0});
            const makeRegion = (i) => api.fromIndexRegion({id:'R'+i, domain:'Смысловой блок '+i,
                members:{OLD:[{id:'o'+i,page:i,type:'TEXT'}],NEW:[{id:'n'+i,page:i+20,type:'TEXT'}]}});
            let model = api.buildModel({regions:Array.from({length:40}, (_,i)=>makeRegion(i+1))});
            model.edges.set('o1|n2', {key:'o1|n2', old_block_id:'o1', new_block_id:'n2', oldPage:1, newPage:22});
            const loaded = new Set(); window.pageReads=[];
            const store = markRaw({state:S, model(){ void S.version; return model; },
                ensureDiagramPage(side,page){
                    const key=side+page; if(loaded.has(key))return; loaded.add(key); window.pageReads.push(key);
                    S.pages[side][page]={status:'ok', geometry:{width_px:1000,height_px:1400},
                        blocks:[{block_id:(side==='OLD'?'o':'n')+(side==='OLD'?page:page-20),bbox:[.1,.1,.9,.7]}]};
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
        await page.waitForFunction(()=>document.querySelectorAll('.sbm-simple__lines path').length===41);
        await page.waitForFunction(()=>document.querySelector('.sbm-simple__crop img')?.naturalWidth>0);
        assert.equal(await page.locator('.sbm-simple__card').count(),80);
        assert.equal(await page.locator('.sbm-bar, .sbm-shelves, .sbm-decision, select, input').count(),0);
        assert(await page.evaluate(()=>window.pageReads.length<20), 'off-screen pages stay unloaded');
        assert.equal(await page.locator('.sbm-simple__lines path').first().evaluate(el=>getComputedStyle(el).strokeDasharray),'7px, 5px');
        const geometry = () => page.evaluate(()=>{
            const canvas=document.querySelector('.sbm-simple__canvas').getBoundingClientRect();
            const old=document.querySelector('[data-block="o1"]').getBoundingClientRect();
            const next=document.querySelector('[data-block="n1"]').getBoundingClientRect();
            const line=document.querySelector('.sbm-simple__lines path');
            const start=line.getPointAtLength(0),end=line.getPointAtLength(line.getTotalLength());
            return {dx:Math.abs(start.x-(old.right-canvas.left)),dy:Math.abs(end.y-(next.top+next.height/2-canvas.top)),
                left:old.left,right:next.left};
        });
        let g=await geometry(); assert(g.dx<1 && g.dy<1 && g.left<g.right);
        await page.locator('[data-block="o1"]').click();
        assert.equal(await page.locator('.sbm-simple__lines path.is-active').count(),2);
        assert.equal(await page.locator('.sbm-simple__card.is-related').count(),3);
        await page.screenshot({path:'/tmp/semantic-block-diagram-desktop.png'});
        await page.locator('.sbm-simple__scroll').evaluate(el=>el.scrollTop=5000);
        await page.waitForFunction(()=>window.pageReads.includes('OLD20'));
        g=await geometry(); assert(g.dx<1 && g.dy<1, 'links stay attached while scrolling');
        await page.setViewportSize({width:600,height:800});
        await page.waitForFunction(()=>Number(document.querySelector('.sbm-simple__lines').getAttribute('width'))<=600);
        g=await geometry(); assert(g.dx<1 && g.dy<1 && g.left<g.right);
        await page.evaluate(()=>window.fixture.mounted.value=false);
        await page.evaluate(()=>window.fixture.mounted.value=true);
        await page.waitForFunction(()=>document.querySelectorAll('.sbm-simple__lines path').length===41);
        await page.evaluate(()=>window.fixture.S.statusError={code:'test'});
        await page.getByRole('alert').waitFor();
        assert.equal(await page.locator('.sbm-simple__card').count(),0);
        await page.getByRole('button',{name:'Повторить'}).click();
        await page.waitForFunction(()=>document.querySelectorAll('.sbm-simple__card').length===80);
        await page.evaluate(()=>window.fixture.empty());
        await page.getByRole('status').waitFor();
        assert.equal(await page.locator('.sbm-simple__lines path').count(),0);
        assert.deepEqual(errors,[]);
        assert(requests.every(r=>r.method==='GET'));
    } finally { await browser.close(); await new Promise(resolve=>server.close(resolve)); }
});
