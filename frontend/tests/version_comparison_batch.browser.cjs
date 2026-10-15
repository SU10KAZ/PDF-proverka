/* Real Vue and browser folder selection; synthetic data and intercepted loopback only. */
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {chromium} = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const frontend = path.resolve(__dirname, '..');
const fallback = process.env.BATCH_BROWSER_BASE_FRONTEND || frontend;
const read = name => fs.readFileSync(fs.existsSync(path.join(frontend,name)) ? path.join(frontend,name) : path.join(fallback,name),'utf8');

test('thirty folders: select, exclude, upload, review and reopen the persisted queue', async () => {
    const fixture=fs.mkdtempSync(path.join(os.tmpdir(),'vc-batch-browser-'));
    for(let i=0;i<30;i++) {
        const dir=path.join(fixture,`Project ${String(i+1).padStart(2,'0')}`,'versions');
        fs.mkdirSync(dir,{recursive:true});
        for(let v=1;v<=2;v++) fs.writeFileSync(path.join(dir,`version${v}.pdf`),'%PDF-1.4 synthetic test');
    }
    const errors=[],requests=[];
    let batch=null;
    const html=`<!doctype html><html><meta charset="utf-8"><link rel="stylesheet" href="/styles.css">
      <link rel="stylesheet" href="/vc.css"><link rel="stylesheet" href="/batch.css">
      <div id="app"><version-comparison-page /></div><script src="/vue.js"></script>
      <script src="/batch.js"></script><script src="/vc.js"></script><script>
      localStorage.setItem('version-comparison:object','obj');
      const app=Vue.createApp({});VersionComparison.register(app);app.mount('#app');</script></html>`;
    const browser=await chromium.launch({headless:true,executablePath:process.env.CHROME_BIN || '/opt/google/chrome/chrome',args:['--no-sandbox']});
    try {
        const page=await browser.newPage({viewport:{width:1360,height:900}});
        page.on('pageerror',e=>errors.push(e.message));
        await page.route('**/*',async route=>{
            const req=route.request(), url=new URL(req.url());
            assert.equal(url.origin,'http://127.0.0.1:33129');
            const files={'/vue.js':'static/js/vue.global.prod.js','/vc.js':'static/js/version-comparison.js',
                '/batch.js':'static/js/version-comparison-batch.js','/styles.css':'static/css/styles.css',
                '/vc.css':'static/css/version-comparison.css','/batch.css':'static/css/version-comparison-batch.css'};
            if(files[url.pathname]) return route.fulfill({contentType:url.pathname.endsWith('.css')?'text/css':'text/javascript',body:read(files[url.pathname])});
            if(url.pathname==='/') return route.fulfill({contentType:'text/html',body:html});
            let data={};
            if(url.pathname.endsWith('/objects')) data={items:[{id:'obj',name:'Test object'}]};
            else if(url.pathname.endsWith('/catalog')) data={projects:[]};
            else if(url.pathname.endsWith('/batches')) {
                if(req.method()==='POST') {
                    const body=req.postDataJSON();
                    requests.push(['create',body]);
                    batch={...body,started:false,paused:false,created_at:'2026-10-15T12:00:00Z',items:body.items.map(i=>({...i,state:'uploading',files:i.files.map(f=>({...f,file_name:f.relative_path.split('/').at(-1),received:false}))}))};
                    data=batch;
                } else data={batches:batch?[batch]:[]};
            } else if(url.pathname.includes('/files/')) {
                const parts=url.pathname.split('/'), iid=parts[parts.indexOf('items')+1], fid=parts[parts.indexOf('files')+1];
                const entry=batch.items.find(i=>i.item_id===iid).files.find(f=>f.file_id===fid);
                entry.received=true;requests.push(['file',fid]);data=entry;
            } else if(url.pathname.endsWith('/start')) {
                assert(batch.items.every(i=>i.files.every(f=>f.received)));
                batch.started=true;batch.items.forEach(i=>{i.state='review';i.files.forEach(f=>f.detected={page_count:2,izm_max:1});});
                requests.push(['start']);data=batch;
            } else if(url.pathname.endsWith('/confirm')) {
                const iid=url.pathname.split('/').at(-2), item=batch.items.find(i=>i.item_id===iid);
                const body=req.postDataJSON();requests.push(['confirm',body]);item.state='ready';data=batch;
            } else if(batch && url.pathname.endsWith('/'+batch.batch_id)) data=batch;
            else throw new Error('Unexpected endpoint: '+url.pathname);
            return route.fulfill({contentType:'application/json',body:JSON.stringify(data)});
        });
        await page.goto('http://127.0.0.1:33129/');
        await page.getByRole('button',{name:'⇪ Загрузить проект со всеми версиями',exact:true}).click();
        await page.getByRole('button',{name:'Загрузить несколько проектов из папок',exact:true}).click();
        const dialog=page.getByRole('dialog',{name:'Загрузка нескольких проектов'});
        await dialog.locator('label').filter({hasText:'Общая папка с проектами'}).locator('input').setInputFiles(fixture);
        await page.waitForFunction(()=>document.querySelectorAll('.vc-batch-list tbody tr').length===30);
        await dialog.getByRole('textbox',{name:'Название проекта'}).first().fill('Renamed first project');
        await dialog.locator('.vc-batch-list tbody tr').nth(1).locator('input[type=checkbox]').uncheck();
        await dialog.getByRole('button',{name:'Загрузить и поставить в очередь',exact:true}).click();
        await dialog.getByRole('button',{name:'Подтвердить состав и порядок',exact:true}).first().waitFor();
        assert.equal(batch.items.length,29);
        assert.equal(batch.items[0].title,'Renamed first project');
        assert.equal(requests.filter(r=>r[0]==='file').length,58);
        const first=dialog.locator('.vc-batch-item').first();
        const originalOrder=batch.items[0].files.map(f=>f.file_id);
        await first.getByRole('button',{name:'Версия ниже',exact:true}).first().click();
        await first.getByRole('button',{name:'Подтвердить состав и порядок',exact:true}).click();
        await page.waitForFunction(()=>document.querySelector('.vc-batch-item')?.textContent.includes('Ожидает обработки'));
        assert.deepEqual(requests.find(r=>r[0]==='confirm')[1].files.map(f=>f.file_id),originalOrder.reverse());
        await dialog.getByRole('button',{name:'Закрыть',exact:true}).click();
        await page.reload();
        await page.locator('.vc-batch-bar select').selectOption(batch.batch_id);
        await dialog.waitFor();
        assert.equal(await dialog.locator('.vc-batch-item').count(),29);
        if(process.env.BATCH_BROWSER_SCREENSHOT) await page.screenshot({path:process.env.BATCH_BROWSER_SCREENSHOT});
        assert.deepEqual(errors,[]);
    } finally {
        await browser.close();fs.rmSync(fixture,{recursive:true,force:true});
    }
});
